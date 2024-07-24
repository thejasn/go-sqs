package message_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/thejasn/go-sqs/consumer/message"
	"github.com/stretchr/testify/suite"
)

type UnitTest struct {
	suite.Suite
}

func TestUnitSuites(t *testing.T) {
	suite.Run(t, &UnitTest{})
}

func (u *UnitTest) TestSQSMessage() {
	sqsMessage := types.Message{
		MessageId:     aws.String("message-id"),
		ReceiptHandle: aws.String("receipt-handle"),
		Body:          aws.String(`{"content": "fake-content"}`),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"attribute1": {
				DataType:    aws.String("String"),
				StringValue: aws.String("value1"),
			},
		},
	}

	message := message.New(sqsMessage)

	u.Equal("message-id", message.Metadata.MessageId)
	u.Equal("receipt-handle", message.Metadata.ReceiptHandle)
	u.Equal(`{"content": "fake-content"}`, message.Content)
}

func (u *UnitTest) TestSNSMessage() {
	snsMessage := types.Message{
		MessageId:     aws.String("message-id"),
		ReceiptHandle: aws.String("receipt-handle"),
		Attributes: map[string]string{
			"ApproximateReceiveCount": "1",
		},
		Body: aws.String(`
			{
				"Message": "{\n  \"asda\": \"asdas\"\n}",
				"MessageAttributes": {
					"attribute1": {
						"Type": "String",
						"Value": "value1"
					}
				}
			}
		`),
	}

	message := message.New(snsMessage)

	u.Equal("message-id", message.Metadata.MessageId)
	u.Equal("receipt-handle", message.Metadata.ReceiptHandle)
	u.Equal("{\n  \"asda\": \"asdas\"\n}", message.Content)
	u.Equal(2, len(message.Metadata.MessageAttributes))
	u.Equal("value1", message.Metadata.MessageAttributes["attribute1"])
	u.Equal("1", message.Metadata.MessageAttributes["ApproximateReceiveCount"])
}

func (u *UnitTest) TestSNSWithoutMessageAttributes() {
	snsMessage := types.Message{
		MessageId:     aws.String("message-id"),
		ReceiptHandle: aws.String("receipt-handle"),
		Body: aws.String(`
			{
				"Message": "{\n  \"asda\": \"asdas\"\n}"
			}
		`),
	}

	message := message.New(snsMessage)

	u.Equal("message-id", message.Metadata.MessageId)
	u.Equal("receipt-handle", message.Metadata.ReceiptHandle)
	u.Equal("{\n  \"asda\": \"asdas\"\n}", message.Content)
	u.Equal(0, len(message.Metadata.MessageAttributes))
}

func (u *UnitTest) TestUnmarshal() {
	snsMessage := types.Message{
		MessageId:     aws.String("message-id"),
		ReceiptHandle: aws.String("receipt-handle"),
		Body: aws.String(`
			{
				"Message": "{\n  \"name\": \"test\"\n}",
				"MessageAttributes": {
					"attribute1": {
						"Type": "String",
						"Value": "value1"
					}
				}
			}
		`),
	}

	message := message.New(snsMessage)

	User := struct {
		Name string `json:"name"`
	}{}

	message.Unmarshal(&User)

	u.Equal("message-id", message.Metadata.MessageId)
	u.Equal("receipt-handle", message.Metadata.ReceiptHandle)
	u.Equal("{\n  \"name\": \"test\"\n}", message.Content)
	u.Equal(1, len(message.Metadata.MessageAttributes))
	u.Equal("value1", message.Metadata.MessageAttributes["attribute1"])

	u.Equal("test", User.Name)
}

func (u *UnitTest) TestUnmarshalWithError() {
	snsMessage := types.Message{
		MessageId:     aws.String("message-id"),
		ReceiptHandle: aws.String("receipt-handle"),
		Body:          aws.String("not a json"),
	}

	message := message.New(snsMessage)

	User := struct {
		Email string `json:"email"`
	}{}

	err := message.Unmarshal(&User)

	u.Equal("message-id", message.Metadata.MessageId)
	u.Equal("receipt-handle", message.Metadata.ReceiptHandle)
	u.Equal("not a json", message.Content)
	u.NotNil(err)
}
