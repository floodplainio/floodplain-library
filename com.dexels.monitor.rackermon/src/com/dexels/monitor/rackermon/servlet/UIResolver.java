package com.dexels.monitor.rackermon.servlet;

import org.osgi.service.component.annotations.Component;


@Component(name="dexels.monitor.uiresolver",service=UIResolver.class, immediate=true, 
property= {"osgi.http.whiteboard.resource.pattern=/","osgi.http.whiteboard.resource.prefix=/resources"})
public class UIResolver {

}
