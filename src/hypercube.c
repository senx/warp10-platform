/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include "hypercube.h"
#include "dimension_vector.h"

/*
 * A hypercube represents the partition bounds of a hypertable chunk.
 *
 * A hypercube consists of N slices that each represent a range in a particular
 * dimension that make up the hypercube. When a new tuple is inserted into a
 * hypertable, and no chunk exists that can hold that tuple, we need to
 * calculate a new hypercube that encloses the point corresponding to the
 * tuple. When calculating the hypercube, we need to account for alignment
 * requirements in dimensions marked as "aligned" and also ensure that there are
 * no collisions with existing chunks. Alignment issues and collisions can occur
 * when the partitioning configuration has changed (e.g., the time interval or
 * number of partitions in a particular dimension changed).
 */
Hypercube *
ts_hypercube_alloc(int16 num_dimensions)
{
	Hypercube *hc = palloc0(HYPERCUBE_SIZE(num_dimensions));

	hc->capacity = num_dimensions;
	return hc;
}

void
ts_hypercube_free(Hypercube *hc)
{
	int i;

	for (i = 0; i < hc->num_slices; i++)
		ts_dimension_slice_free(hc->slices[i]);

	pfree(hc);
}

#if defined(USE_ASSERT_CHECKING)
static inline bool
hypercube_is_sorted(Hypercube *hc)
{
	int i;

	if (hc->num_slices < 2)
		return true;

	for (i = 1; i < hc->num_slices; i++)
		if (hc->slices[i]->fd.dimension_id < hc->slices[i - 1]->fd.dimension_id)
			return false;

	return true;
}
#endif

Hypercube *
ts_hypercube_copy(Hypercube *hc)
{
	Hypercube *copy;
	size_t nbytes = HYPERCUBE_SIZE(hc->capacity);
	int i;

	copy = palloc(nbytes);
	memcpy(copy, hc, nbytes);

	for (i = 0; i < hc->num_slices; i++)
		copy->slices[i] = ts_dimension_slice_copy(hc->slices[i]);

	return copy;
}

static int
cmp_slices_by_dimension_id(const void *left, const void *right)
{
	const DimensionSlice *left_slice = *((DimensionSlice **) left);
	const DimensionSlice *right_slice = *((DimensionSlice **) right);

	if (left_slice->fd.dimension_id == right_slice->fd.dimension_id)
		return 0;
	if (left_slice->fd.dimension_id < right_slice->fd.dimension_id)
		return -1;
	return 1;
}

void
ts_hypercube_add_slice(Hypercube *hc, DimensionSlice *slice)
{
	Assert(hc->capacity > hc->num_slices);

	hc->slices[hc->num_slices++] = slice;

	/* Check if we require a sort to maintain dimension order */
	if (hc->num_slices > 1 &&
		slice->fd.dimension_id < hc->slices[hc->num_slices - 2]->fd.dimension_id)
		ts_hypercube_slice_sort(hc);

	Assert(hypercube_is_sorted(hc));
}

/*
 * Sort the hypercubes slices in ascending dimension ID order. This allows us to
 * iterate slices in a consistent order.
 */
void
ts_hypercube_slice_sort(Hypercube *hc)
{
	qsort(hc->slices, hc->num_slices, sizeof(DimensionSlice *), cmp_slices_by_dimension_id);
}

DimensionSlice *
ts_hypercube_get_slice_by_dimension_id(Hypercube *hc, int32 dimension_id)
{
	DimensionSlice slice = {
		.fd.dimension_id = dimension_id,
	};
	void *ptr = &slice;

	if (hc->num_slices == 0)
		return NULL;

	Assert(hypercube_is_sorted(hc));

	ptr = bsearch(&ptr,
				  hc->slices,
				  hc->num_slices,
				  sizeof(DimensionSlice *),
				  cmp_slices_by_dimension_id);

	if (NULL == ptr)
		return NULL;

	return *((DimensionSlice **) ptr);
}

/*
 * Given a set of constraints, build the corresponding hypercube.
 */
Hypercube *
ts_hypercube_from_constraints(ChunkConstraints *constraints, MemoryContext mctx)
{
	Hypercube *hc;
	int i;
	MemoryContext old;

	old = MemoryContextSwitchTo(mctx);
	hc = ts_hypercube_alloc(constraints->num_dimension_constraints);
	MemoryContextSwitchTo(old);

	for (i = 0; i < constraints->num_constraints; i++)
	{
		ChunkConstraint *cc = chunk_constraints_get(constraints, i);

		if (is_dimension_constraint(cc))
		{
			DimensionSlice *slice;

			Assert(hc->num_slices < constraints->num_dimension_constraints);
			slice = ts_dimension_slice_scan_by_id(cc->fd.dimension_slice_id, mctx);
			Assert(slice != NULL);
			hc->slices[hc->num_slices++] = slice;
		}
	}

	ts_hypercube_slice_sort(hc);

	Assert(hypercube_is_sorted(hc));

	return hc;
}

/*
 * Calculate the hypercube that encloses the given point.
 *
 * The hypercube's dimensions are calculated one by one, and depend on the
 * current partitioning in each dimension of the N-dimensional hyperspace,
 * including any alignment requirements.
 *
 * For non-aligned dimensions, we simply calculate the hypercube's slice range
 * in that dimension given current partitioning configuration. If there is
 * already an identical slice for that dimension, we will reuse it rather than
 * creating a new one.
 *
 * For aligned dimensions, we first try to find an existing slice that covers
 * the insertion point. If an existing slice is found, we reuse it or otherwise
 * we calculate a new slice as described for non-aligned dimensions.
 *
 * If a hypercube has dimension slices that are not reused ones, we might need
 * to cut them to ensure alignment and avoid collisions with other chunk
 * hypercubes. This happens in a later step.
 */
Hypercube *
ts_hypercube_calculate_from_point(Hyperspace *hs, Point *p)
{
	Hypercube *cube;
	int i;

	cube = ts_hypercube_alloc(hs->num_dimensions);

	/* For each dimension, calculate the hypercube's slice in that dimension */
	for (i = 0; i < hs->num_dimensions; i++)
	{
		Dimension *dim = &hs->dimensions[i];
		int64 value = p->coordinates[i];
		bool found = false;

		/* Assert that dimensions are in ascending order */
		Assert(i == 0 || dim->fd.id > hs->dimensions[i - 1].fd.id);

		/*
		 * If this is an aligned dimension, we'd like to reuse any existing
		 * slice that covers the coordinate in the dimension
		 */
		if (dim->fd.aligned)
		{
			DimensionVec *vec;

			vec = ts_dimension_slice_scan_limit(dim->fd.id, value, 1);

			if (vec->num_slices > 0)
			{
				cube->slices[i] = vec->slices[0];
				found = true;
			}
		}

		if (!found)
		{
			/*
			 * No existing slice found, or we are not aligning, so calculate
			 * the range of a new slice
			 */
			cube->slices[i] = ts_dimension_calculate_default_slice(dim, value);

			/*
			 * Check if there's already an existing slice with the calculated
			 * range. If a slice already exists, use that slice's ID instead
			 * of a new one.
			 */
			ts_dimension_slice_scan_for_existing(cube->slices[i]);
		}
	}

	cube->num_slices = hs->num_dimensions;

	Assert(hypercube_is_sorted(cube));

	return cube;
}

/*
 * Check if two hypercubes collide (overlap).
 *
 * This is basically an axis-aligned bounding box collision detection,
 * generalized to N dimensions. We check for dimension slice collisions in each
 * dimension and only if all dimensions collide there is a hypercube collision.
 */
bool
ts_hypercubes_collide(Hypercube *cube1, Hypercube *cube2)
{
	int i;

	Assert(cube1->num_slices == cube2->num_slices);

	for (i = 0; i < cube1->num_slices; i++)
		if (!ts_dimension_slices_collide(cube1->slices[i], cube2->slices[i]))
			return false;

	return true;
}
