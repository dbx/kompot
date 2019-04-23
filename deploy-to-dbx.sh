#!/bin/bash

TAG=
GROUP="hu/dbx"
ARTIFACT="kompot"

while [ $# -gt 0 ]
        do
                case $1 in
                        --tag) TAG=$2
                                shift
                                ;;
                        -*)
                            echo "WARN: Unknown option (ignored): $1" >&2
                            shift
                        ;;
                        *)  # no more options. Stop while loop
                            break
                            ;;
                esac
                shift
        done

if [ -z "$TAG" ]; then
   echo "Tag version is necessary!"
   exit -1
fi

echo "Uploading version $TAG to maven!"
git checkout $TAG

echo "-----------------------------------------------"

mvn clean package

# ha elszállt a maven task, akkor ne csináljunk semmit.
if [ "$?" -ne 0 ] ; then
  exit -1
fi

cd ./target
a=$(find . -type f -name 'kompot-[0-9]*.[0-9]*.[0-9]*.jar')

b=${a#*-}
VERSION=${b%.*}

cp ../pom.xml ./$ARTIFACT-$VERSION.pom

REMOTE_DIRECTORY="/home/ubuntu/maven/repository/$GROUP/$ARTIFACT/$VERSION"

shasum $ARTIFACT-$VERSION.jar > $ARTIFACT-$VERSION.jar.sha1
shasum $ARTIFACT-$VERSION.pom > $ARTIFACT-$VERSION.pom.sha1

cd ..

ssh -i ~/.ssh/dbx.pem ubuntu@services.dbx.hu "mkdir -p $REMOTE_DIRECTORY"
scp -i ~/.ssh/dbx.pem ./target/$ARTIFACT-$VERSION.jar ./target/$ARTIFACT-$VERSION.jar.sha1 ./target/$ARTIFACT-$VERSION.pom ./target/$ARTIFACT-$VERSION.pom.sha1 ubuntu@services.dbx.hu:$REMOTE_DIRECTORY