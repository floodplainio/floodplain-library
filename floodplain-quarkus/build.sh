/Library/Java/JavaVirtualMachines/graalvm-ce-java8-19.3.1/Contents/Home/bin/native-image \
	-J-Duser.language=en \
	-J-Dfile.encoding=UTF-8 \
        --initialize-at-build-time=com.google.protobuf \
	'-H:InitialCollectionPolicy=com.oracle.svm.core.genscavenge.CollectionPolicy$BySpaceAndTime' \
	-H:+JNI -jar build/floodplain-quarkus-0.6.2-runner.jar \
	-H:FallbackThreshold=0 \
	-H:+ReportExceptionStackTraces \
	-H:-AddAllCharsets \
	-H:-IncludeAllTimeZones \
	-H:-SpawnIsolates \
	--no-server \
	-H:-UseServiceLoaderFeature \
	-H:+StackTrace build/floodplain-quarkus-0.6.2-runner \
	--allow-incomplete-classpath
