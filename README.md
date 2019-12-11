# Dexels Repository developer guide

This repository is an OSGi repository that is built using bnd / bndtools. Within Eclipse, it uses the bndtools plugin, for noninteractive builds it uses bnd, integrated with gradle. Even though this repository does not use Maven at all, Bnd+Gradle and bndtools still integrate with Maven repositories, so we can use the same repositories to retrieve and store artifacts.


The global project configuration is in the cnf/ project. The build.bnd points to a few Maven repositories, which it uses to retrieve (and store) dependencies.

If we take a look at the build.bnd file:

```
# Configure Repositories
-plugin.4.Release: \
	aQute.bnd.repository.maven.provider.MavenBndRepository; \
		releaseUrl=http://repo.dexels.com/repository/dexelsrepo/; \
		index=${build}/releaserepo.maven; \
		name = Release
	
-plugin.5.LocalBuild: \
	aQute.bnd.repository.maven.provider.MavenBndRepository; \
		releaseUrl=https://repo.maven.apache.org/maven2/; \
		index=${build}/thirdparty.maven; \
		name="Dex"

-plugin.6.Internal: \
	aQute.bnd.repository.maven.provider.MavenBndRepository; \
		releaseUrl=http://repo.dexels.com/repository/thirdparty/; \
		index=${build}/internal.maven; \
		name="Internal"

-releaserepo: Release
-baselinerepo: Release


javac.source:          1.8
javac.target:          1.8
# JUnit
junit: org.apache.servicemix.bundles.junit; version=4.12
```

We see that it points to three maven repo's, most on our repo.dexels.com machine.
First we see the http://repo.dexels.com/repository/dexelsrepo/ repository. The name of this repository ('Release') matches the '-releaserepo' setting at the end of the file, so we know this is the release repository. This means that if we release a bundle (using Eclipse or Gradle), this is where that bundle will end up.

Second there is a public https://repo.maven.apache.org/maven2/ server, which serves publicly available jars.

Finally there is the http://repo.dexels.com/repository/thirdparty/ repo. This one is mostly used when we need a 'special' version of a dependency, perhaps to work around a bug.

All these repo's also point to a file (e.g. internal.maven) and these list all the artefacts bnd(tools) can use. So if we want to add or update depedencies, this is where we need to do that for this project.
(Keep in mind that these dependencies do not influence which bundles will be added to container images, that you will need to keep in sync with the dexels-base project)



### Getting started

For using bndtools in eclipse I suggest you use a separate, clean eclipse. Don't download the eclipse installer, just download the Java Enterprise package. Other combinations probably will work, but your mileage might vary, and this way it is easier to upgrade (just download a new eclipse, point to a new workspace, import again, and if all is well, throw away the old eclipse and workspace, if not, you can still use the previous setup until you figure out what's wrong).

Then, install bndtools from the marketplace. I'm at 4.1, current is at this moment 4.3, seems to work fine. For now, avoid gradle integration of Eclipse ('buildship') it seems to mess with bndtools.

Clone from git: https://github.com/Dexels/dexels.repository
Import all projects.

### Compiling

All projects should compile now. If it does not, check the repo.dexels.com server, if that has failed it can not resolve its dependencies.

Every project has a bnd.bnd file. It looks something like this:
```
-buildpath: \
	osgi.core,\
	osgi.cmpn,\
	org.osgi.service.component.annotations,\
	com.dexels.oauth.api;version=latest,\
	slf4j.api;version=1.7
Bundle-Version: 2.1.5
Private-Package: \
	com.dexels.oauth.impl
-dsannotations:  \
	*
Import-Package: *
```
First of all it lists the dependencies it should use to compile and build the bundle. For the rest this file is the basis bnd uses to generate your MANIFEST.MF bundle file, and it will inspect the dependencies to see which packages need to be imported. Also, it will export packages you have labeled as being 'exported'. The bndtools bnd editor helps nicely with that.

If a project compiles successfully, it will create a jar file in the generated/ folder of the project.
Bndtools also gives you a nice 'jar viewer'. You can double click on a bundle and see what it imports / exports, this can be pretty valuable to troubleshoot OSGi resolution issues.

### Components
Another advantage of Bnd over PDE/Tycho is that Bnd will generate the OSGI-INF service definition files based on annotations. The exact way this all works (Using the @Component, @Reference and @Activate annotations) is a bit out of scope here, but when creating complicated dependency graphs of services it is much easier to work with than PDE/Tycho.

### Running locally

You should also be able to run local run configurations (*.bndrun). These are slightly different than the regular eclipse launch configurations, but essentially they define a OSGI framework version, a list of bundles, and a few other switches.

A fun feature of bndtools is that if you edit code and save it while an OSGi process is running, Eclipse will not only rebuild the jar file, it will also re-install the bundle in the runtime. This allows you to just never really need to restart, at least if you run everything using all the OSGi rules.

This can be switched off, in some cases this is not what you want.

### Building from command line:
You should also be able to build from gradle, I haven't tested from Windows, but I'm pretty confident it will work. You can also use the 'gradlew' shell script, that should download a reasonable version if needed.

To build from command line:
 - (If gradle is installed) gradle clean build

Or else:
 - (From the repository root) ./gradlew clean build

Releasing:

For releasing, you will need to have the right credentials in your bnd settings:
(This is AFAIK the same as the ~/.m2/settings.xml maven settings, I think you can just copy that file)

Make sure you have the following file in ~/.bnd/settings.xml:
```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                          http://maven.apache.org/xsd/settings-1.0.0.xsd">
	<servers>
		<server>
			<id>http://repo.dexels.com/repository/dexelsrepo/</id>
			<username>deployment</username>
			<password>xxxxxxxxx</password>
		</server>
	</servers>
</settings>
```
And add the follwing to the ~/.m2/settings.xml:
```xml
		<server>
                       <id>http://*.dexels.com</id>
                       <username>deployment</username>
                       <password>xxxxxxxxxx</password>
                       <configuration>
                               <timeout>120000</timeout> <!-- your timeout in milliseconds -->
                       </configuration>
               </server>
```
(And replace the xxxxxxx with the correct password)

## Releasing
Now we can release a bundle from Eclipse.
Right click on a project, choose release. You will get a screen that will compare the local version you're about to release to the baseline version (I think that's the previous released version). It will show what version numbers need to change (obviously the bundle version, but also package versions if exported package have been changed). 

Beware that this detection acts weirdly at times, sometimes it suggest lowering versions (I guess that is a bug. In that case, make sure to increase it instead, or not release at all). In any case, look at this 

Bnd(tools) is pretty pedantic on semantic versioning. So consumer incompatible changes (like removing a method from an interface) will cause a MAJOR version update, supplier incompatible changes (like adding a method to an interface) will case a MINOR update, and the rest will cause a PATCH update.

After you have released the bundle, check if it is present in the repo.dexels.com machine.

If you made changes to multiple dependent bundles, make sure you release them from upstream to downstream. As the generated MANIFESTs will only accept a pretty specific range, you otherwise might end up with incompatible bundle versions.

## Releasing from Gradle
From command line:
 - Enter the project dir and call 'gradle release' (from a project folder to release a specific bundle, or from the root to release all bundles that need releasing)

Note: I've never really gotten this to work reliably, I think it works the same as the Eclipse based release process, but there you get a bit more incremental and fine grained control.

Ideally I would release from CircleCI (basically that any change pushed to master that builds and tests successfully will trigger a release.)

So either way, now our released bundle should be on the repo.dexels.com server.

From there we can use it inside a container or in the target platform (see the dexels-base repository), or refer to it in another way.
