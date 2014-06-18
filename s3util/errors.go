package s3util

import (
	"github.com/crowdmob/goamz/s3"
)

// List of all AWS S3 error codes, extracted from:
//    http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	ErrAccessDenied                            = "AccessDenied"
	ErrAccountProblem                          = "AccountProblem"
	ErrAmbiguousGrantByEmailAddress            = "AmbiguousGrantByEmailAddress"
	ErrBadDigest                               = "BadDigest"
	ErrBucketAlreadyExists                     = "BucketAlreadyExists"
	ErrBucketAlreadyOwnedByYou                 = "BucketAlreadyOwnedByYou"
	ErrBucketNotEmpty                          = "BucketNotEmpty"
	ErrCredentialsNotSupported                 = "CredentialsNotSupported"
	ErrCrossLocationLoggingProhibited          = "CrossLocationLoggingProhibited"
	ErrEntityTooSmall                          = "EntityTooSmall"
	ErrEntityTooLarge                          = "EntityTooLarge"
	ErrExpiredToken                            = "ExpiredToken"
	ErrIllegalVersioningConfigurationException = "IllegalVersioningConfigurationException"
	ErrIncompleteBody                          = "IncompleteBody"
	ErrIncorrectNumberOfFilesInPostRequest     = "IncorrectNumberOfFilesInPostRequest"
	ErrInlineDataTooLarge                      = "InlineDataTooLarge"
	ErrInternalError                           = "InternalError"
	ErrInvalidAccessKeyId                      = "InvalidAccessKeyId"
	ErrInvalidAddressingHeader                 = "InvalidAddressingHeader"
	ErrInvalidArgument                         = "InvalidArgument"
	ErrInvalidBucketName                       = "InvalidBucketName"
	ErrInvalidBucketState                      = "InvalidBucketState"
	ErrInvalidDigest                           = "InvalidDigest"
	ErrInvalidLocationConstraint               = "InvalidLocationConstraint"
	ErrInvalidObjectState                      = "InvalidObjectState"
	ErrInvalidPart                             = "InvalidPart"
	ErrInvalidPartOrder                        = "InvalidPartOrder"
	ErrInvalidPayer                            = "InvalidPayer"
	ErrInvalidPolicyDocument                   = "InvalidPolicyDocument"
	ErrInvalidRange                            = "InvalidRange"
	ErrInvalidRequest                          = "InvalidRequest"
	ErrInvalidSecurity                         = "InvalidSecurity"
	ErrInvalidSOAPRequest                      = "InvalidSOAPRequest"
	ErrInvalidStorageClass                     = "InvalidStorageClass"
	ErrInvalidTargetBucketForLogging           = "InvalidTargetBucketForLogging"
	ErrInvalidToken                            = "InvalidToken"
	ErrInvalidURI                              = "InvalidURI"
	ErrKeyTooLong                              = "KeyTooLong"
	ErrMalformedACLError                       = "MalformedACLError"
	ErrMalformedPOSTRequest                    = "MalformedPOSTRequest"
	ErrMalformedXML                            = "MalformedXML"
	ErrMaxMessageLengthExceeded                = "MaxMessageLengthExceeded"
	ErrMaxPostPreDataLengthExceededError       = "MaxPostPreDataLengthExceededError"
	ErrMetadataTooLarge                        = "MetadataTooLarge"
	ErrMethodNotAllowed                        = "MethodNotAllowed"
	ErrMissingAttachment                       = "MissingAttachment"
	ErrMissingContentLength                    = "MissingContentLength"
	ErrMissingRequestBodyError                 = "MissingRequestBodyError"
	ErrMissingSecurityElement                  = "MissingSecurityElement"
	ErrMissingSecurityHeader                   = "MissingSecurityHeader"
	ErrNoLoggingStatusForKey                   = "NoLoggingStatusForKey"
	ErrNoSuchBucket                            = "NoSuchBucket"
	ErrNoSuchKey                               = "NoSuchKey"
	ErrNoSuchLifecycleConfiguration            = "NoSuchLifecycleConfiguration"
	ErrNoSuchUpload                            = "NoSuchUpload"
	ErrNoSuchVersion                           = "NoSuchVersion"
	ErrNotImplemented                          = "NotImplemented"
	ErrNotSignedUp                             = "NotSignedUp"
	ErrNotSuchBucketPolicy                     = "NotSuchBucketPolicy"
	ErrOperationAborted                        = "OperationAborted"
	ErrPermanentRedirect                       = "PermanentRedirect"
	ErrPreconditionFailed                      = "PreconditionFailed"
	ErrRedirect                                = "Redirect"
	ErrRestoreAlreadyInProgress                = "RestoreAlreadyInProgress"
	ErrRequestIsNotMultiPartContent            = "RequestIsNotMultiPartContent"
	ErrRequestTimeout                          = "RequestTimeout"
	ErrRequestTimeTooSkewed                    = "RequestTimeTooSkewed"
	ErrRequestTorrentOfBucketError             = "RequestTorrentOfBucketError"
	ErrSignatureDoesNotMatch                   = "SignatureDoesNotMatch"
	ErrServiceUnavailable                      = "ServiceUnavailable"
	ErrSlowDown                                = "SlowDown"
	ErrTemporaryRedirect                       = "TemporaryRedirect"
	ErrTokenRefreshRequired                    = "TokenRefreshRequired"
	ErrTooManyBuckets                          = "TooManyBuckets"
	ErrUnexpectedContent                       = "UnexpectedContent"
	ErrUnresolvableGrantByEmailAddress         = "UnresolvableGrantByEmailAddress"
	ErrUserKeyMustBeSpecified                  = "UserKeyMustBeSpecified"
)

// IsS3Error checks if an error is from the S3 API.
func IsS3Error(err error, awsCode string) bool {
	s3err, ok := err.(*s3.Error)
	if !ok {
		return false
	}
	return s3err.Code == awsCode
}
