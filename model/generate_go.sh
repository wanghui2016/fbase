#!/usr/bin/env bash
. ./common.sh

check_protoc_version

PROGRAM=$(basename "$0")

#FBASEPATH=$(pwd)/../../../..
FBASEPATH=/home/young/github
FBASEPATH=$(readlink -e "$FBASEPATH")
echo "FBAS PATH: "$FBASEPATH

export GOPATH=$FBASEPATH

GO_PREFIX_PATH=model/pkg
VENDORPATH=$FBASEPATH/src/vendor

gogo_protobuf_url=github.com/gogo/protobuf
GOGO_ROOT=$VENDORPATH/${gogo_protobuf_url}
GO_OUT_M=
GO_INSTALL='go install'

cmd_exists () {
    which "$1" 1>/dev/null 2>&1
}

echo "install gogoproto code/generator ..."

# link gogo to GOPATH
mkdir -p $FBASEPATH/src/$(dirname "$gogo_protobuf_url")
ln -snf $GOGO_ROOT $FBASEPATH/src/$gogo_protobuf_url

# install gogo
${GO_INSTALL} ${gogo_protobuf_url}/proto
${GO_INSTALL} ${gogo_protobuf_url}/protoc-gen-gofast
${GO_INSTALL} ${gogo_protobuf_url}/gogoproto

echo "install goimports ..."
goimports_url="golang.org/x/tools/cmd/goimports"
mkdir -p $FBASEPATH/src/$(dirname "$goimports_url")
ln -snf $VENDORPATH/${goimports_url} $FBASEPATH/src/${goimports_url}
ln -snf $VENDORPATH $FBASEPATH/src/${goimports_url}/vendor
${GO_INSTALL} ${goimports_url}

# add the bin path of gogoproto generator into PATH if it's missing
if ! cmd_exists protoc-gen-gofast; then
    for path in $(echo "${GOPATH}" | sed -e 's/:/ /g'); do
        gogo_proto_bin="${path}/bin/protoc-gen-gofast"
        if [ -e "${gogo_proto_bin}" ]; then
            export PATH=$(dirname "${gogo_proto_bin}"):$PATH
            break
        fi
    done
fi

cd proto
for file in `ls *.proto`
    do
    base_name=$(basename $file ".proto")
    mkdir -p ../pkg/$base_name
    if [ -z $GO_OUT_M ]; then
        GO_OUT_M="M$file=$GO_PREFIX_PATH/$base_name"
    else
        GO_OUT_M="$GO_OUT_M,M$file=$GO_PREFIX_PATH/$base_name"
    fi
done

echo "generate go code..."
ret=0
for file in `ls *.proto`
    do
    base_name=$(basename $file ".proto")
    protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gofast_out=plugins=grpc,$GO_OUT_M:../pkg/$base_name $file || ret=$?
    cd ../pkg/$base_name
    sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
    sed -i.bak -E 's/import fmt \"fmt\"//g' *.pb.go
    sed -i.bak -E 's/import io \"io\"//g' *.pb.go
    sed -i.bak -E 's/import math \"math\"//g' *.pb.go
    rm -f *.bak
    goimports -w *.pb.go
    cd ../../proto
done
exit $ret
