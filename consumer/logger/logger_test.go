package logger_test

import (
	"testing"

	"github.com/thejasn/go-sqs/consumer/logger"
	"github.com/stretchr/testify/suite"
)

type UnitTestSuite struct {
	suite.Suite
}

func TestUnitSuites(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}

func (ut *UnitTestSuite) TestNew() {
	logger := logger.New(logger.DefaultLoggerConfig{LogLevel: "info"})

	ut.NotNil(logger)
}

func (ut *UnitTestSuite) TestLog() {
	logger := logger.New(logger.DefaultLoggerConfig{LogLevel: "info"})

	logger.Log("test")

	ut.NotNil(logger)
}

func (ut *UnitTestSuite) TestFallbackLogLevel() {
	logger := logger.New(logger.DefaultLoggerConfig{LogLevel: "invalid"})

	ut.NotNil(logger)
	ut.Equal("info", logger.LogLevel)
}
