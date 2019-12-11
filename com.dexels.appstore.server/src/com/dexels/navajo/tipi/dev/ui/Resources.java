package com.dexels.navajo.tipi.dev.ui;

import org.osgi.service.component.annotations.Component;


@Component(name="appstore.server.ui",service=Resources.class, immediate=true, 
property= {"osgi.http.whiteboard.resource.pattern=/ui/*",
"osgi.http.whiteboard.resource.prefix=/ui"})
public class Resources {

}
