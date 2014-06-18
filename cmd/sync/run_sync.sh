#!/bin/sh
set -e

# export AWS_ACCESS_KEY=`cat $HOME/key/aws.access`
# export AWS_SECRET_KEY=`cat $HOME/key/aws.secret`
# export BUCKET_SOURCE="s3://shopify-perf/"
# export BUCKET_DEST="s3://shopify-brigade-test/lolz"

export AWS_ACCESS_KEY=`cat .aws/aws.access`
export AWS_SECRET_KEY=`cat .aws/aws.secret`
export BUCKET_SOURCE="s3://shopify/s"
export BUCKET_DEST="s3://shopify-copy/"

set -x

./brigade-s3-sync \
    -src=$BUCKET_SOURCE \
    -dst=$BUCKET_DEST   \
    -para=300 \
    -filename="bucket_list.json.gz"

