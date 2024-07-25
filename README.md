[![Maintainability](https://api.codeclimate.com/v1/badges/9693cf5c61dc08d04fd5/maintainability)](https://codeclimate.com/github/inaciogu/go-sqs-consumer/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/9693cf5c61dc08d04fd5/test_coverage)](https://codeclimate.com/github/inaciogu/go-sqs-consumer/test_coverage)

## Go SQS Consumer

### 🌟Description
This is a simple package to help you consume messages from AWS SQS.

### 🚀Features
- [x] Consume messages in parallel
- [x] Consume messages from different defined queues
- [x] Consume messages from different queues by a prefix
- [x] Error handling
- [x] Message unmarshalling
- [x] Message deletion
- [x] Logging


### Installation
To install the package, use the following command:

``````shell
go get github.com/inaciogu/go-sqs/consumer
``````

### Usage

``````go
package main

import (
	"fmt"

	"github.com/inaciogu/go-sqs/consumer"
	"github.com/inaciogu/go-sqs/consumer/handler"
	"github.com/inaciogu/go-sqs/consumer/message"
	"github.com/joho/godotenv"
)

type Message struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

func main() {
	godotenv.Load(".env")

	consumer1 := consumer.New(nil, consumer.SQSClientOptions{
		QueueName: "test_queue",
		Handle: func(message *message.Message) bool {
			myMessage := Message{}

			// Unmarshal the message content
			err := message.Unmarshal(&myMessage)

			if err != nil {
				fmt.Println(err)

				// Do something if the message content cannot be unmarshalled
				return false
			}

			fmt.Println(myMessage.Email)

			return true
		},
		WaitTimeSeconds: 30,
		Region:                 "us-east-1",
	})

	consumer1.Start()
	// Or
	handler.New([]consumer.SQSClientInterface{
		consumer1,
		// consumer 2, consumer 3, ...
	}).Run()
}

``````
If you want to consume queues by a prefix, you can just set the `PrefixBased` option to `true` Then, the `QueueName` will be used as a prefix to find all queues that match the prefix.

### Configuration
To give the package access to your AWS account, you can use the following environment variables:

``````shell
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
``````

### Contribution
If you want to contribute to the development of this package, follow these steps:

- Fork the repository
- Create a new branch (git checkout -b feature/new-feature)
- Commit your changes (git commit -m 'Add new feature')
- Push to the branch (git push origin feature/new-feature)
- Open a Pull Request

#### Development 
Regenerate mocks using mockery.

### Running locally
To use this package locally (without using your own AWS account) you can execute the `docker compose up` command that will run the [localstack](https://www.localstack.cloud/) and execute terraform commands to deploy the infra configured in `/iac/terraform/main.tf` locally.

### License
This package is distributed under the **MIT** license. See the LICENSE file for more information.

### Contact
Gustavo Inacio - [Linkedin](https://linkedin.com/in/inaciogu)
