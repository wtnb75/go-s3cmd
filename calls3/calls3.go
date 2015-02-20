package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/smartystreets/go-aws-auth"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

func dumphdr(r http.Header) {
	for k, v := range r {
		for _, vv := range v {
			fmt.Printf("%s: %s\n", k, vv)
		}
	}
	fmt.Println()
}

func apprun(c *cli.Context) {
	ufn := c.String("upload-file")
	var err error
	var rd io.Reader
	switch ufn {
	case "-":
		rd = os.Stdin
	case "":
		rd = nil
	default:
		rd, err = os.Open(ufn)
		if err != nil {
			log.Println("open file", rd, err)
		}
	}
	cred := awsauth.Credentials{
		AccessKeyID:     c.String("access_key"),
		SecretAccessKey: c.String("secret_key"),
	}
	for _, url := range c.Args() {
		req, err := http.NewRequest(c.String("method"), url, rd)
		for _, hdr := range c.StringSlice("header") {
			hdra := strings.SplitN(hdr, ":", 2)
			req.Header.Add(hdra[0], strings.TrimSpace(hdra[1]))
		}
		awsauth.SignS3(req, cred)
		if c.Bool("dump-header") {
			dumphdr(req.Header)
		}
		client := new(http.Client)
		res, err := client.Do(req)
		if err != nil {
			log.Println("request", url, err)
		}
		if c.Bool("dump-header") {
			dumphdr(res.Header)
		}
		io.Copy(os.Stdout, res.Body)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	app := cli.NewApp()
	app.Name = "s3call"
	app.Usage = "s3 API caller"
	app.Author = ""
	app.Email = ""
	app.Version = "0.1.0"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "method,m,X",
			Usage: "Method [GET|PUT|POST|DELETE|OPTIONS]",
			Value: "GET",
		},
		cli.StringFlag{
			Name:  "upload-file,T",
			Usage: "Transfer FILE to destination",
		},
		cli.StringFlag{
			Name:   "access_key,access-key,a",
			Usage:  "Access Key ID",
			EnvVar: "AWS_ACCESS_KEY,AWS_ACCESS_KEY_ID",
		},
		cli.BoolFlag{
			Name:  "dump-header,D",
			Usage: "Dump HTTP Header",
		},
		cli.StringFlag{
			Name:   "secret_key,secret-key,s",
			Usage:  "Access Secret Key",
			EnvVar: "AWS_SECRET_ACCESS_KEY,AWS_SECRET_KEY",
		},
		cli.StringSliceFlag{
			Name:  "header,H",
			Value: &cli.StringSlice{},
			Usage: "Add HTTP Header",
		},
	}
	app.Action = apprun
	app.Run(os.Args)
}
