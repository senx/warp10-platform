include "io_warp10_continuum_thrift_data.thrift"
namespace java io.warp10.continuum.thrift.service

service GeoDirectoryService {
  /**
   * Filter GTS ids using a Geo shape
   */
  io_warp10_continuum_thrift_data.GeoDirectoryResponse filter(1:io_warp10_continuum_thrift_data.GeoDirectoryRequest request);
}
