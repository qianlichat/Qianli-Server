package org.whispersystems.textsecuregcm.controllers;

import org.signal.storageservice.providers.ProtocolBufferMediaType;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;

@Path("/ping")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Ping")
public class PingController {
  public PingController(){}

  @GET
  @Produces(ProtocolBufferMediaType.APPLICATION_PROTOBUF)
  @Consumes(ProtocolBufferMediaType.APPLICATION_PROTOBUF)
  public CompletableFuture<Response> ping(){
    return CompletableFuture.completedFuture(Response.status(Response.Status.OK).build());
  }
}
