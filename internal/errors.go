package pubsub

type ErrTopicAlreadyExists struct{}

func (e ErrTopicAlreadyExists) Error() string {
	return "topic already exists"
}

type ErrTopicNotFound struct{}

func (e ErrTopicNotFound) Error() string {
	return "topic not exists"
}

type ErrSubscriptionAlreadyExists struct{}

func (e ErrSubscriptionAlreadyExists) Error() string {
	return "subscription already exists"
}

type ErrSubscriptionNotFound struct{}

func (e ErrSubscriptionNotFound) Error() string {
	return "subscription not exists"
}
