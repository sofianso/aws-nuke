package awsmod

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws/awserr"
)

const (
	// DefaultBatchSize is the batch size we initialize when constructing a batch delete client.
	// This value is used when calling DeleteObjects. This represents how many objects to delete
	// per DeleteObjects call.
	DefaultBatchSize = 1000
)

// BatchError will contain the key and bucket of the object that failed to
// either upload or download.
type BatchError struct {
	Errors  Errors
	code    string
	message string
}

// Errors is a typed alias for a slice of errors to satisfy the error
// interface.
type Errors []Error

func (errs Errors) Error() string {
	buf := bytes.NewBuffer(nil)
	for i, err := range errs {
		buf.WriteString(err.Error())
		if i+1 < len(errs) {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

// Error will contain the original error, bucket, and key of the operation that failed
// during batch operations.
type Error struct {
	OrigErr error
	Bucket  *string
	Key     *string
}

func newError(err error, bucket, key *string) Error {
	return Error{
		err,
		bucket,
		key,
	}
}

func (err *Error) Error() string {
	origErr := ""
	if err.OrigErr != nil {
		origErr = ":\n" + err.OrigErr.Error()
	}
	return fmt.Sprintf("failed to perform batch operation on %q to %q%s",
		aws.ToString(err.Key),
		aws.ToString(err.Bucket),
		origErr,
	)
}

// NewBatchError will return a BatchError that satisfies the awserr.Error interface.
func NewBatchError(code, message string, err []Error) awserr.Error {
	return &BatchError{
		Errors:  err,
		code:    code,
		message: message,
	}
}

// Code will return the code associated with the batch error.
func (err *BatchError) Code() string {
	return err.code
}

// Message will return the message associated with the batch error.
func (err *BatchError) Message() string {
	return err.message
}

func (err *BatchError) Error() string {
	return awserr.SprintError(err.Code(), err.Message(), "", err.Errors)
}

// OrigErr will return the original error. Which, in this case, will always be nil
// for batched operations.
func (err *BatchError) OrigErr() error {
	return err.Errors
}

// BatchDeleteIterator is an interface that uses the scanner pattern to
// iterate through what needs to be deleted.
type BatchDeleteIterator interface {
	Next() bool
	Err() error
	DeleteObject() BatchDeleteObject
}

// DeleteListIterator is an alternative iterator for the BatchDelete client. This will
// iterate through a list of objects and delete the objects.
//
// Example:
//
//	iter := &s3manager.DeleteListIterator{
//		Client: svc,
//		Input: &s3.ListObjectsInput{
//			Bucket:  aws.String("bucket"),
//			MaxKeys: aws.Int64(5),
//		},
//		Paginator: request.Pagination{
//			NewRequest: func() (*request.Request, error) {
//				var inCpy *ListObjectsInput
//				if input != nil {
//					tmp := *input
//					inCpy = &tmp
//				}
//				req, _ := c.ListObjectsRequest(inCpy)
//				return req, nil
//			},
//		},
//	}
//
//	batcher := s3manager.NewBatchDeleteWithClient(svc)
//	if err := batcher.Delete(aws.BackgroundContext(), iter); err != nil {
//		return err
//	}
type DeleteListIterator struct {
	Bucket    *string
	Paginator *s3.ListObjectsV2Paginator
	objects   []s3types.Object
	err       error
}

// NewDeleteListIterator will return a new DeleteListIterator.
func NewDeleteListIterator(
	svc s3.ListObjectsV2APIClient, input *s3.ListObjectsV2Input, opts ...func(*DeleteListIterator),
) BatchDeleteIterator {
	iter := &DeleteListIterator{
		Bucket:    input.Bucket,
		Paginator: s3.NewListObjectsV2Paginator(svc, input),
	}

	for _, opt := range opts {
		opt(iter)
	}
	return iter
}

// Next will use the S3API client to iterate through a list of objects.
func (iter *DeleteListIterator) Next() bool {
	if len(iter.objects) > 0 {
		iter.objects = iter.objects[1:]
		if len(iter.objects) > 0 {
			return true
		}
	}

	if !iter.Paginator.HasMorePages() {
		return false
	}

	page, err := iter.Paginator.NextPage(context.TODO())
	if err != nil {
		iter.err = err
		return false
	}

	iter.objects = page.Contents
	return len(iter.objects) > 0
}

// Err will return the last known error from Next.
func (iter *DeleteListIterator) Err() error {
	return iter.err
}

// DeleteObject will return the current object to be deleted.
func (iter *DeleteListIterator) DeleteObject() BatchDeleteObject {
	return BatchDeleteObject{
		Object: &s3.DeleteObjectInput{
			Bucket: iter.Bucket,
			Key:    iter.objects[0].Key,
		},
	}
}

// DeleteObjectsAPIClient implements the S3.DeleteObjects operation.
type DeleteObjectsAPIClient interface {
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

// BatchDelete will use the s3 package's service client to perform a batch
// delete.
type BatchDelete struct {
	Client    DeleteObjectsAPIClient
	BatchSize int
}

// NewBatchDeleteWithClient will return a new delete client that can delete a batched amount of
// objects.
//
// Example:
//
//	batcher := s3manager.NewBatchDeleteWithClient(client, size)
//
//	objects := []BatchDeleteObject{
//		{
//			Object:	&s3.DeleteObjectInput {
//				Key: aws.String("key"),
//				Bucket: aws.String("bucket"),
//			},
//		},
//	}
//
//	if err := batcher.Delete(aws.BackgroundContext(), &s3manager.DeleteObjectsIterator{
//		Objects: objects,
//	}); err != nil {
//		return err
//	}
func NewBatchDeleteWithClient(s3client DeleteObjectsAPIClient, batchSize int, options ...func(*BatchDelete)) *BatchDelete {
	svc := &BatchDelete{
		Client:    s3client,
		BatchSize: DefaultBatchSize,
	}

	if batchSize != -1 {
		svc.BatchSize = batchSize
	}

	for _, opt := range options {
		opt(svc)
	}

	return svc
}

// NewBatchDelete will return a new delete client that can delete a batched amount of
// objects.
//
// Example:
//
//	batcher := s3manager.NewBatchDelete(sess, size)
//
//	objects := []BatchDeleteObject{
//		{
//			Object:	&s3.DeleteObjectInput {
//				Key: aws.String("key"),
//				Bucket: aws.String("bucket"),
//			},
//		},
//	}
//
//	if err := batcher.Delete(aws.BackgroundContext(), &s3manager.DeleteObjectsIterator{
//		Objects: objects,
//	}); err != nil {
//		return err
//	}
func NewBatchDelete(c *aws.Config, batchSize int, options ...func(*BatchDelete)) *BatchDelete {
	s3client := s3.NewFromConfig(*c)

	return NewBatchDeleteWithClient(s3client, batchSize, options...)
}

// BatchDeleteObject is a wrapper object for calling the batch delete operation.
type BatchDeleteObject struct {
	Object *s3.DeleteObjectInput
	// After will run after each iteration during the batch process. This function will
	// be executed whether the request was successful.
	After func() error
}

// DeleteObjectsIterator is an interface that uses the scanner pattern to iterate
// through a series of objects to be deleted.
type DeleteObjectsIterator struct {
	Objects []BatchDeleteObject
	index   int
	inc     bool
}

// Next will increment the default iterators index and ensure that there
// is another object to iterator to.
func (iter *DeleteObjectsIterator) Next() bool {
	if iter.inc {
		iter.index++
	} else {
		iter.inc = true
	}
	return iter.index < len(iter.Objects)
}

// Err will return an error. Since this is just used to satisfy the BatchDeleteIterator interface
// this will only return nil.
func (iter *DeleteObjectsIterator) Err() error {
	return nil
}

// DeleteObject will return the BatchDeleteObject at the current batched index.
func (iter *DeleteObjectsIterator) DeleteObject() BatchDeleteObject {
	object := iter.Objects[iter.index]
	return object
}

// Delete will use the iterator to queue up objects that need to be deleted.
// Once the batch size is met, this will call the deleteBatch function.
func (d *BatchDelete) Delete(ctx context.Context, iter BatchDeleteIterator, opts ...func(input *s3.DeleteObjectsInput)) error {
	var errs []Error
	var objects []BatchDeleteObject
	var input *s3.DeleteObjectsInput

	for iter.Next() {
		o := iter.DeleteObject()

		if input == nil {
			input = initDeleteObjectsInput(o.Object)
		}

		for _, opt := range opts {
			opt(input)
		}

		parity := hasParity(input, o)
		if parity {
			input.Delete.Objects = append(input.Delete.Objects, s3types.ObjectIdentifier{
				Key:       o.Object.Key,
				VersionId: o.Object.VersionId,
			})
			objects = append(objects, o)
		}

		if len(input.Delete.Objects) == d.BatchSize || !parity {
			if err := deleteBatch(ctx, d, input, objects); err != nil {
				errs = append(errs, err...)
			}

			objects = objects[:0]
			input = nil

			if !parity {
				objects = append(objects, o)
				input = initDeleteObjectsInput(o.Object)

				for _, opt := range opts {
					opt(input)
				}

				input.Delete.Objects = append(input.Delete.Objects, s3types.ObjectIdentifier{
					Key:       o.Object.Key,
					VersionId: o.Object.VersionId,
				})
			}
		}
	}

	// iter.Next() could return false (above) plus populate iter.Err()
	if iter.Err() != nil {
		errs = append(errs, newError(iter.Err(), nil, nil))
	}

	if input != nil && len(input.Delete.Objects) > 0 {
		if err := deleteBatch(ctx, d, input, objects); err != nil {
			errs = append(errs, err...)
		}
	}

	if len(errs) > 0 {
		return NewBatchError("BatchedDeleteIncomplete", "some objects have failed to be deleted.", errs)
	}
	return nil
}

func initDeleteObjectsInput(o *s3.DeleteObjectInput) *s3.DeleteObjectsInput {
	return &s3.DeleteObjectsInput{
		Bucket:       o.Bucket,
		MFA:          o.MFA,
		RequestPayer: o.RequestPayer,
		Delete:       &s3types.Delete{},
	}
}

const (
	// ErrDeleteBatchFailCode represents an error code which will be returned
	// only when DeleteObjects.Errors has an error that does not contain a code.
	ErrDeleteBatchFailCode       = "DeleteBatchError"
	errDefaultDeleteBatchMessage = "failed to delete"
)

// deleteBatch will delete a batch of items in the objects parameters.
func deleteBatch(ctx context.Context, d *BatchDelete, input *s3.DeleteObjectsInput, objects []BatchDeleteObject) []Error {
	var errs []Error

	if result, err := d.Client.DeleteObjects(ctx, input); err != nil {
		for i := 0; i < len(input.Delete.Objects); i++ {
			errs = append(errs, newError(err, input.Bucket, input.Delete.Objects[i].Key))
		}
	} else if len(result.Errors) > 0 {
		for i := 0; i < len(result.Errors); i++ {
			code := ErrDeleteBatchFailCode
			msg := errDefaultDeleteBatchMessage
			if result.Errors[i].Message != nil {
				msg = *result.Errors[i].Message
			}
			if result.Errors[i].Code != nil {
				code = *result.Errors[i].Code
			}

			errs = append(errs, newError(awserr.New(code, msg, err), input.Bucket, result.Errors[i].Key))
		}
	}
	for _, object := range objects {
		if object.After == nil {
			continue
		}
		if err := object.After(); err != nil {
			errs = append(errs, newError(err, object.Object.Bucket, object.Object.Key))
		}
	}

	return errs
}

func hasParity(o1 *s3.DeleteObjectsInput, o2 BatchDeleteObject) bool {
	if o1.Bucket != nil && o2.Object.Bucket != nil {
		if *o1.Bucket != *o2.Object.Bucket {
			return false
		}
	} else if o1.Bucket != o2.Object.Bucket {
		return false
	}

	if o1.MFA != nil && o2.Object.MFA != nil {
		if *o1.MFA != *o2.Object.MFA {
			return false
		}
	} else if o1.MFA != o2.Object.MFA {
		return false
	}

	if o1.RequestPayer != "" && o2.Object.RequestPayer != "" {
		if o1.RequestPayer != o2.Object.RequestPayer {
			return false
		}
	} else if o1.RequestPayer != o2.Object.RequestPayer {
		return false
	}

	return true
}
