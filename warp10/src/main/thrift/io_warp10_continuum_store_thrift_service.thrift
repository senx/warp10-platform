include "io_warp10_continuum_store_thrift_data.thrift"
namespace java io.warp10.continuum.store.thrift.service

service DirectoryService {
  /**
   * Find Metadatas of matching GTS instances
   */
  io_warp10_continuum_store_thrift_data.DirectoryFindResponse find(1:io_warp10_continuum_store_thrift_data.DirectoryFindRequest request);
  
  /**
   * Retrieve Metadata associated with a given classId/labelsId
   */
  io_warp10_continuum_store_thrift_data.DirectoryGetResponse get(1:io_warp10_continuum_store_thrift_data.DirectoryGetRequest request);
  
  /**
   * Retrieve statistics associated with selected GTS instances
   */
  io_warp10_continuum_store_thrift_data.DirectoryStatsResponse stats(1:io_warp10_continuum_store_thrift_data.DirectoryStatsRequest request);    
}
