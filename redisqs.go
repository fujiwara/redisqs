package redisqs

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"gopkg.in/redis.v5"
)

var (
	DelayedBuffer = time.Second * 180
)

type Job struct {
	key       string
	Payload   string    `json:"payload"`
	TimeStamp time.Time `json:"timestamp"`
}

func (j Job) Digest() string {
	h := sha256.New()
	io.WriteString(h, j.Payload)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ParseRedisURL parses url for redis (redis://host:port/db)
func ParseRedisURL(s string) (*redis.Options, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "redis" {
		return nil, errors.New("invalid scheme")
	}
	op := &redis.Options{}
	h, p, err := net.SplitHostPort(u.Host)
	if err != nil {
		h = u.Host
		p = "6379"
	}
	op.Network = "tcp"
	op.Addr = h + ":" + p
	if u.Path == "" || u.Path == "/" {
		op.DB = 0
	} else {
		ps := strings.Split(u.Path, "/")
		if len(ps) > 1 {
			i, err := strconv.Atoi(ps[1])
			if err != nil {
				return nil, fmt.Errorf("invalid database %s", ps[1])
			}
			op.DB = i
		} else {
			op.DB = 0
		}
	}
	return op, nil
}

type Client struct {
	redisClient *redis.Client
	sqsURL      string
}

func NewClient(redisURL string, sqsURL string) (*Client, error) {
	op, err := ParseRedisURL(redisURL)
	if err != nil {
		return nil, err
	}
	return &Client{
		redisClient: redis.NewClient(op),
		sqsURL:      sqsURL,
	}, nil
}

func (c *Client) Close() error {
	return c.redisClient.Close()
}

func (c *Client) Send(jobs []Job) error {
	svc := sqs.New(session.New())
	for _, job := range jobs {
		params := &sqs.SendMessageInput{
			MessageBody: aws.String(job.Payload),
			QueueUrl:    aws.String(c.sqsURL),
			//			MessageDeduplicationId: aws.String(job.Digest()),
		}
		delay := job.TimeStamp.Sub(time.Now())
		if ds := delay.Seconds(); ds > 0 {
			params.DelaySeconds = aws.Int64(int64(ds))
		}
		log.Printf("send %#v", params)
		sqsRes, err := svc.SendMessage(params)
		if err != nil {
			return err
		}
		log.Printf("sent %s", *sqsRes.MessageId)

		res := c.redisClient.ZRem(job.key, job.Payload)
		if err := res.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) Fetch(key string) ([]Job, error) {
	zr := redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(
			time.Now().Add(DelayedBuffer).Unix(),
			10,
		),
	}
	log.Printf("fetch %s %#v", key, zr)
	res := c.redisClient.ZRangeByScoreWithScores(key, zr)
	if res.Err() != nil {
		return nil, res.Err()
	}
	zs, err := res.Result()
	if err != nil {
		return nil, err
	}
	jobs := make([]Job, 0, len(zs))
	for _, z := range zs {
		if s, ok := z.Member.(string); ok {
			job := Job{
				key:       key,
				Payload:   s,
				TimeStamp: time.Unix(int64(z.Score), 0),
			}
			log.Printf("job %#v", job)
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}
