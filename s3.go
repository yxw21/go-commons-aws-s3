package go_commons_aws_s3

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"net/url"
)

type Client struct {
	s3 *s3.Client
}

func (ctx *Client) CreateBucket(bucket string) (*s3.CreateBucketOutput, error) {
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}
	return ctx.s3.CreateBucket(context.TODO(), input)
}

func (ctx *Client) DeleteBucket(bucket string) (*s3.DeleteBucketOutput, error) {
	input := &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	}
	return ctx.s3.DeleteBucket(context.TODO(), input)
}

func (ctx *Client) UploadObject(bucket, key string, body io.Reader, contentType, cacheControl string) (*manager.UploadOutput, error) {
	uploader := manager.NewUploader(ctx.s3)
	return uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         body,
		ContentType:  aws.String(contentType),
		CacheControl: aws.String(cacheControl),
	})
}

func (ctx *Client) DeleteObject(bucket, key string) (*s3.DeleteObjectOutput, error) {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	return ctx.s3.DeleteObject(context.TODO(), input)
}

func (ctx *Client) CopyObject(sourceBucket, key, destinationBucket string) (*s3.CopyObjectOutput, error) {
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(url.PathEscape(sourceBucket)),
		CopySource: aws.String(destinationBucket),
		Key:        aws.String(key),
	}
	return ctx.s3.CopyObject(context.TODO(), input)
}

func (ctx *Client) GetObjectSign(bucket, key string) (*v4.PresignedHTTPRequest, error) {
	preSignClient := s3.NewPresignClient(ctx.s3)
	return preSignClient.PresignGetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
}

func (ctx *Client) ListBuckets() (*s3.ListBucketsOutput, error) {
	input := &s3.ListBucketsInput{}
	return ctx.s3.ListBuckets(context.TODO(), input)
}

func (ctx *Client) ListObjects(bucket string) (*s3.ListObjectsOutput, error) {
	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}
	return ctx.s3.ListObjects(context.TODO(), input)
}

func (ctx *Client) GetS3() *s3.Client {
	return ctx.s3
}

func NewClient(region, key, secret, session string) (*Client, error) {
	if region == "" {
		return nil, errors.New("region cannot be empty")
	}
	if key == "" {
		return nil, errors.New("key cannot be empty")
	}
	if secret == "" {
		return nil, errors.New("secret cannot be empty")
	}
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, session)),
	)
	if err != nil {
		return nil, err
	}
	return &Client{
		s3: s3.NewFromConfig(cfg),
	}, nil
}
