package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

// S3Store is an implementation of Store backed by AWS S3.
type S3Store struct {
	profile string
	region  string
	bucket  string
	client  *s3.S3
}

func init() {
	registerBuilder("s3", func(builder *Builder, config map[string]interface{}) (store Store, err error) {
		var profile, region, bucket string
		profile, err = builder.getString(config, "profile")
		if err != nil {
			return
		}
		region, err = builder.getString(config, "region")
		if err != nil {
			return
		}
		bucket, err = builder.getString(config, "bucket")
		if err != nil {
			return
		}
		return NewS3Store(profile, region, bucket)
	})
}

func NewS3Store(profile, region, bucket string) (*S3Store, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      &region,
		Credentials: credentials.NewSharedCredentials("", profile),
	})
	if err != nil {
		return nil, err
	}
	return &S3Store{
		profile: profile,
		region:  region,
		bucket:  bucket,
		client:  s3.New(sess),
	}, nil
}

func (s *S3Store) Get(key []byte) (value []byte, err error) {
	hexKey := fmt.Sprintf("%x", key)
	output, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(hexKey),
	})
	if err != nil {
		if rfErr, ok := err.(awserr.RequestFailure); ok {
			if rfErr.StatusCode() == http.StatusNotFound {
				return nil, fmt.Errorf("%q: %w", key, ErrNotFound)
			}
		}
		return nil, err
	}
	defer func() {
		if err := output.Body.Close(); err != nil {
			log.WithFields(log.Fields{
				"op":  "get",
				"key": hexKey,
			}).Warning("Could not close response body")
		}
	}()
	return ioutil.ReadAll(output.Body)
}

func (s *S3Store) Put(key, value []byte) (err error) {
	hexKey := fmt.Sprintf("%x", key)
	_, err = s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(hexKey),
		Body:   bytes.NewReader(value),
	})
	return
}
