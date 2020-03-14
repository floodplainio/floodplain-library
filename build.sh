/Library/Java/JavaVirtualMachines/graalvm-ce-java8-19.3.1/Contents/Home/bin/native-image \
	-J-Duser.language=en \
	-J-Dfile.encoding=UTF-8 \
	--initialize-at-build-time= \
	'-H:InitialCollectionPolicy=com.oracle.svm.core.genscavenge.CollectionPolicy$BySpaceAndTime' \
	-H:+JNI -jar build/floodplain-library-unspecified-runner.jar \
	-H:FallbackThreshold=0 \
	-H:+ReportExceptionStackTraces \
	-H:-AddAllCharsets \
	-H:-IncludeAllTimeZones \
	-H:-SpawnIsolates \
	--no-server \
	-H:-UseServiceLoaderFeature \
	-H:+StackTrace build/floodplain-library-unspecified-runner \
	--allow-incomplete-classpath
