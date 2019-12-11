package com.dexels.oauth.web.generic;

import org.osgi.service.component.annotations.Component;

@Component(name = "oauthwebresources", service = Resource.class, immediate = true, property = {
        "osgi.http.whiteboard.resource.pattern=/auth/*", "osgi.http.whiteboard.resource.prefix=/resources" })

public class Resource {

}
