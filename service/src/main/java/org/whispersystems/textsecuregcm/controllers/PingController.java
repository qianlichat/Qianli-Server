package org.whispersystems.textsecuregcm.controllers;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/ping")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Ping")
public class PingController {
  public PingController(){}

  @GET
  public void ping(){

  }
}
