#!/usr/bin/env bash
NAME="kvrocks"
VERSION=`_build/bin/kvrocks -v|awk '{printf $2;}'`
STAGE=${STAGE:-release}
fpm -f -s dir -t rpm --prefix '/www/kvrocks'  -n ${NAME} --epoch 7 \
    --config-files /www/kvrocks/conf/kvrocks.conf \
    -v ${VERSION} --iteration ${CI_PIPELINE_ID}.${STAGE} -C ./_build \
    --verbose --category 'kvrockslabs/projects' --description 'kvrocks' \
    --url 'https://github.com/kvrockslabs' --license 'BSD'

