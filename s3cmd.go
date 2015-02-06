package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/vaughan0/go-ini"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Config struct {
	AccessKey        string `json:"access_key_id"`
	SecretKey        string `json:"secret_access_key"`
	Endpoint         string `json:"endpoint"`
	StorageAPI       string `json:"storage_api"`
	Debug            bool   `json:"debug"`
	Force_path_style bool   `json:"force_path_style"`
}

var verbose bool = false
var s3cl *s3.S3

func usage() {
	fmt.Println("Usage:", path.Base(os.Args[0]), "subcommand", "args...")
}

func url2bktpath(s3cl *s3.S3, ustr string) (*s3.Bucket, string, error) {
	u, err := url.Parse(ustr)
	if err != nil {
		log.Fatal("url parse", err)
	}
	if u.Scheme != "s3" && u.Scheme != "dag" {
		return nil, "", fmt.Errorf("invalid scheme: %s", u.Scheme)
	}
	bkt := s3cl.Bucket(u.Host)
	key := u.Path
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}
	return bkt, key, nil
}

func mb(c *cli.Context) {
	setup(c)
	args := c.Args()
	for _, us := range args {
		bkt, _, err := url2bktpath(s3cl, us)
		err = bkt.PutBucket(s3.Private)
		if err != nil {
			log.Fatal("PutBucket", err)
		}
	}
}

func rb(c *cli.Context) {
	setup(c)
	args := c.Args()
	for _, us := range args {
		bkt, _, err := url2bktpath(s3cl, us)
		err = bkt.DelBucket()
		if err != nil {
			log.Fatal("DelBucket", err)
		}
	}
}

func lsshowd(bkt *s3.Bucket, k string, longfmt bool) {
	fmt.Printf("%24s %10s  s3://%s/%s\n", "", "DIR", bkt.Name, k)
}

func lsshow(bkt *s3.Bucket, k s3.Key, longfmt bool) {
	if longfmt {
		fmt.Printf("%v %10d  %s %s s3://%s/%s\n", k.LastModified, k.Size, k.ETag, k.Owner.DisplayName, bkt.Name, k.Key)
	} else {
		fmt.Printf("%v %10d  s3://%s/%s\n", k.LastModified, k.Size, bkt.Name, k.Key)
	}
}

func ls(c *cli.Context) {
	setup(c)
	if len(c.Args()) == 0 {
		// GetService
		gs, err := s3cl.GetService()
		if err != nil {
			log.Fatal("GetService", gs, err)
		}
		fmt.Println("Owner:", gs.Owner.DisplayName)
		u, _ := url.Parse("s3://dummy")
		for _, b := range gs.Buckets {
			u.Host = b.Name
			fmt.Printf("%v  %s\n", b.CreationDate, u)
		}
	} else {
		for _, us := range c.Args() {
			bkt, prefix, err := url2bktpath(s3cl, us)
			if err != nil {
				log.Fatal("invalid url:", err)
			}
			var marker string
			delim := "/"
			if c.Bool("recursive") {
				delim = ""
			}
			for {
				rsp, err := bkt.List(prefix, delim, marker, 1000)
				if err != nil {
					log.Println("error List", err)
					break
				}
				// log.Printf("list result: %+v", rsp)
				for _, k := range rsp.CommonPrefixes {
					lsshowd(bkt, k, c.Bool("long"))
				}
				for _, k := range rsp.Contents {
					// log.Printf("%+v\n", k)
					lsshow(bkt, k, c.Bool("long"))
				}
				marker = rsp.NextMarker
				if !rsp.IsTruncated {
					break
				}
			}
		}
	}
}

func la(c *cli.Context) {
	setup(c)
	gs, err := s3cl.GetService()
	if err != nil {
		log.Fatal("GetService ", err)
	}
	for _, b := range gs.Buckets {
		bkt := s3cl.Bucket(b.Name)
		var marker string
		for {
			rsp, err := bkt.List("", "", marker, 1000)
			if err != nil {
				log.Println("error List", err)
				break
			}
			for _, k := range rsp.Contents {
				fmt.Printf("%v %10d  s3://%s/%s\n", k.LastModified, k.Size, bkt.Name, k.Key)
			}
			marker = rsp.NextMarker
			if !rsp.IsTruncated {
				break
			}
		}
	}
}

func exists(c *cli.Context) {
	setup(c)
	args := c.Args()
	for _, us := range args {
		bkt, key, err := url2bktpath(s3cl, us)
		if err != nil {
			log.Fatal("url", err)
		}
		r, err := bkt.Exists(key)
		if err != nil {
			log.Fatal("head", err)
		}
		if r {
			fmt.Println(us)
		} else {
			log.Println(us, "does not exists")
			os.Exit(1)
		}
	}
}

func head(c *cli.Context) {
	setup(c)
	args := c.Args()
	for _, us := range args {
		bkt, key, err := url2bktpath(s3cl, us)
		if err != nil {
			log.Fatal("url parse ", us, err)
		}
		r, err := bkt.Head(key, map[string][]string{})
		if err != nil {
			log.Fatal("head", r, err)
		}
		r.Write(os.Stdout)
		// fmt.Printf("%+v %+v\n", u, r)
	}
}

func reader_s3(s3cl *s3.S3, urlstr string, hdr http.Header) (res io.ReadCloser) {
	bkt, key, err := url2bktpath(s3cl, urlstr)
	if err != nil {
		log.Fatal("url parse ", urlstr, err)
	}
	rsp, err := bkt.GetResponseWithHeaders(key, hdr)
	if err != nil || rsp == nil {
		log.Fatal("reader error ", urlstr, err)
		return nil
	}
	return rsp.Body
}

func cat(c *cli.Context) {
	setup(c)
	args := c.Args()
	for _, us := range args {
		if c.Bool("recursive") {
			res := lists3(us, "")
			for k, v := range res {
				if v.size != 0 {
					rd := reader_s3(s3cl, us+k, make(http.Header))
					io.Copy(os.Stdout, rd)
					rd.Close()
				}
			}
		} else {
			rd := reader_s3(s3cl, us, make(http.Header))
			io.Copy(os.Stdout, rd)
			rd.Close()
		}
	}
}

func get(c *cli.Context) {
	setup(c)
	args := c.Args()
	for _, us := range args {
		outf := path.Base(us)
		fmt.Println("start get", us, "=>", outf)
		st := time.Now()
		ofp, err := os.Create(outf)
		if err != nil {
			log.Fatal("create", err)
		}
		rd := reader_s3(s3cl, us, make(http.Header))
		ncp, _ := io.Copy(ofp, rd)
		ofp.Close()
		rd.Close()
		fmt.Println("finished", time.Since(st), ncp)
	}
}

func catrange(c *cli.Context) {
	setup(c)
	args := c.Args()
	// range=NNN-YYY
	rstr := c.String("range")
	for _, us := range args {
		hdr := make(http.Header)
		hdr.Set("Range", "bytes="+rstr)
		log.Printf("hdr=%+v\n", hdr)
		rd := reader_s3(s3cl, us, hdr)
		io.Copy(os.Stdout, rd)
		rd.Close()
	}
}

func put(c *cli.Context) {
	setup(c)
	ctyp := c.String("content-type")
	args := c.Args()
	dst := args[len(args)-1]
	src := args[0 : len(args)-1]
	dstbkt, dstbase, err := url2bktpath(s3cl, dst)
	if err != nil {
		log.Fatal("url parse ", dst, err)
	}
	for _, s := range src {
		dstkey := dstbase
		if len(src) != 1 {
			dstkey = path.Join(dstbase, path.Base(s))
		}
		if ifp, err := os.Open(s); err == nil {
			fmt.Printf("start put %s => s3://%s/%s\n", s, dstbkt.Name, dstkey)
			fi, _ := ifp.Stat()
			st := time.Now()
			dstbkt.PutReader(dstkey, ifp, fi.Size(), ctyp, s3.Private, s3.Options{})
			ifp.Close()
			fmt.Println("finished", time.Since(st), fi.Size())
		}
	}
}

func cp(c *cli.Context) {
	setup(c)
	args := c.Args()
	dst := args[len(args)-1]
	src := args[0 : len(args)-1]
	dstbkt, dstbase, err := url2bktpath(s3cl, dst)
	if err != nil {
		log.Fatal("url parse ", dst, err)
	}
	for _, s := range src {
		dstkey := dstbase
		if len(src) != 1 {
			dstkey = path.Join(dstbase, path.Base(s))
		}
		srcbkt, srckey, err := url2bktpath(s3cl, s)
		if err != nil {
			log.Fatal("url parse ", dst, err)
		}
		log.Printf("copy %s => s3://%s/%s", s, dstbkt.Name, dstkey)
		res, err := dstbkt.PutCopy(dstkey, s3.Private, s3.CopyOptions{}, fmt.Sprintf("/%s/%s", srcbkt.Name, srckey))
		if err != nil {
			log.Println("putcopy", res, err)
		}
	}
}

func del(c *cli.Context) {
	setup(c)
	args := c.Args()
	for _, s := range args {
		if c.Bool("recursive") {
			res := lists3(s, "")
			objs := s3.Delete{Quiet: true}
			for k, _ := range res {
				urltodel := s + k
				_, key, _ := url2bktpath(s3cl, urltodel)
				objs.Objects = append(objs.Objects, s3.Object{Key: key})
			}
			bkt, _, err := url2bktpath(s3cl, s)
			err = bkt.DelMulti(objs)
			log.Println("delmulti", err)
		} else {
			bkt, key, err := url2bktpath(s3cl, s)
			if err != nil {
				log.Fatal("url parse ", s, err)
			}
			err = bkt.Del(key)
			log.Println("delete:", s, err)
		}
	}
}

func du(c *cli.Context) {
	setup(c)
	var total_cnt, total_sz int64
	for _, us := range c.Args() {
		bkt, prefix, err := url2bktpath(s3cl, us)
		if err != nil {
			log.Fatal("invalid url:", err)
		}
		var marker string
		var cnt, sz int64
		for {
			rsp, err := bkt.List(prefix, "", marker, 1000)
			if err != nil {
				log.Println("error List", err)
				break
			}
			for _, k := range rsp.Contents {
				cnt += 1
				sz += k.Size
			}
			marker = rsp.NextMarker
			if !rsp.IsTruncated {
				break
			}
		}
		fmt.Printf("%12d %6d s3://%s/%s\n", sz, cnt, bkt.Name, prefix)
		total_cnt += cnt
		total_sz += sz
	}
	if len(c.Args()) > 1 {
		fmt.Printf("%12d %6d total\n", total_sz, total_cnt)
	}
}

func da(c *cli.Context) {
	setup(c)
	gs, err := s3cl.GetService()
	if err != nil {
		log.Fatal("GetService ", err)
	}
	var total_sz, total_cnt int64
	for _, b := range gs.Buckets {
		bkt := s3cl.Bucket(b.Name)
		var sz, cnt int64
		var marker string
		for {
			rsp, err := bkt.List("", "", marker, 1000)
			if err != nil {
				log.Println("error List", err)
				break
			}
			for _, k := range rsp.Contents {
				cnt += 1
				sz += k.Size
			}
			marker = rsp.NextMarker
			if !rsp.IsTruncated {
				break
			}
		}
		fmt.Printf("%12d %6d s3://%s\n", sz, cnt, bkt.Name)
		total_cnt += cnt
		total_sz += sz
	}
	fmt.Printf("%12d %6d total\n", total_sz, total_cnt)
}

func putmulti(c *cli.Context) {
	setup(c)
	args := c.Args()
	// 16MB split upload
	var sepsz int64
	sepsz = int64(c.Int("split"))
	dst := args[len(args)-1]
	src := args[0 : len(args)-1]
	dstbkt, dstbase, err := url2bktpath(s3cl, dst)
	if err != nil {
		log.Fatal("url parse ", dst, err)
	}
	for _, s := range src {
		dstkey := dstbase
		if len(src) != 1 {
			dstkey = path.Join(dstbase, path.Base(s))
		}
		if ifp, err := os.Open(s); err == nil {
			fi, err := ifp.Stat()
			if err != nil {
				log.Println("stat failed", err)
				ifp.Close()
				continue
			}
			st := time.Now()
			if fi.Size() > sepsz {
				fmt.Printf("multipart upload %s => s3://%s/%s\n", s, dstbkt.Name, dstkey)
				multi, err := dstbkt.InitMulti(dstkey, c.String("content-type"), s3.Private, s3.Options{})
				log.Println("initmulti:", multi, err)
				parts, err := multi.PutAll(ifp, sepsz)
				log.Println("putall:", parts, err)
				err = multi.Complete(parts)
				log.Println("complete:", err)
			} else {
				fmt.Printf("normal put %s => s3://%s/%s\n", s, dstbkt.Name, dstkey)
				dstbkt.PutReader(dstkey, ifp, fi.Size(), c.String("content-type"), s3.Private, s3.Options{})
			}
			ifp.Close()
			fmt.Println("finished", time.Since(st), fi.Size())
		}
	}
}

func listmulti(c *cli.Context) {
	setup(c)
	args := c.Args()
	log.Println("list unfinished multipart uploads.")
	for _, dst := range args {
		dstbkt, dstbase, err := url2bktpath(s3cl, dst)
		if err != nil {
			log.Fatal("url parse ", dst, err)
		}
		multis, prefx, err := dstbkt.ListMulti(dstbase, "/")
		log.Println("listmulti", dst, multis, prefx, err)
	}
}

func putpart_sub(parts []s3.Part, multi *s3.Multi, buf *bytes.Buffer) ([]s3.Part, error) {
	if buf.Len() == 0 {
		return parts, nil
	}
	rdbuf := bytes.NewReader(buf.Bytes())
	log.Println("putpart", len(parts), buf.Len())
	part, err := multi.PutPart(len(parts)+1, rdbuf)
	if err != nil {
		return parts, err
	}
	parts = append(parts, part)
	buf.Reset()
	return parts, nil
}

func merge(c *cli.Context) {
	setup(c)
	args := c.Args()
	dst := args[0]
	src := args[1:]
	log.Println("src", src, "dst", dst)
	if len(src) == 0 {
		log.Println("empty source")
		return
	}
	dstbkt, dstbase, err := url2bktpath(s3cl, dst)
	if err != nil {
		log.Fatal("url parse ", dst, err)
	}
	log.Println("dst", dstbkt, dstbase)
	srcurls := map[string]entry{}
	for _, s := range src {
		res := lists3(s, "")
		log.Println("srcfiles", s, len(res))
		for k, v := range res {
			if v.size == 0 {
				continue
			}
			srcurls[s+k] = v
		}
	}
	var down, copy int
	var downsz, copysz int64
	for _, v := range srcurls {
		if v.size > 5*1024*1024 {
			copy += 1
			copysz += v.size
		} else {
			down += 1
			downsz += v.size
		}
	}
	log.Println("down", down, downsz)
	log.Println("copy", copy, copysz)
	if c.Bool("dry-run") {
		return
	}
	urllist := []string{}
	for k, _ := range srcurls {
		urllist = append(urllist, k)
	}
	sort.Strings(urllist)
	var buf bytes.Buffer
	parts := []s3.Part{}
	multi, err := dstbkt.InitMulti(dstbase, c.String("content-type"), s3.Private, s3.Options{})
	if err != nil {
		log.Fatal("init multi ", dstbase, err)
	}
	for _, s := range urllist {
		v := srcurls[s]
		if v.size > 5*1024*1024 {
			parts, err = putpart_sub(parts, multi, &buf)
			if err != nil {
				log.Fatal("putpart ", err)
			}
			srcbkt, srcbase, err := url2bktpath(s3cl, s)
			if err != nil {
				log.Fatal("src ", s, err)
			}
			srcbktstr := srcbkt.Name
			log.Println("copy", s)
			res, part, err := multi.PutPartCopy(len(parts)+1, s3.CopyOptions{}, path.Join(srcbktstr, srcbase))
			if err != nil {
				log.Fatal("PutPartCopy ", s, err, res)
			}
			parts = append(parts, part)
		} else {
			log.Println("read", s)
			rsz, err := buf.ReadFrom(reader_s3(s3cl, s, make(http.Header)))
			if rsz != v.size || err != nil {
				log.Fatal("copy error ", s, rsz, err)
			}
			if buf.Len() > 8*1024*1024 {
				parts, err = putpart_sub(parts, multi, &buf)
				if err != nil {
					log.Fatal("putpartsub ", err)
				}
			}
		}
	}
	parts, err = putpart_sub(parts, multi, &buf)
	if err != nil {
		log.Fatal("PutPart(last) ", err)
	}
	err = multi.Complete(parts)
	log.Println("complete", err)
}

type aclpol struct {
	Owner struct {
		ID          string
		DisplayName string
	}
	AccessControlList struct {
		Grant []struct {
			Grantee struct {
				ID          string
				DisplayName string
			}
			Permission string
		}
	}
}

func info(c *cli.Context) {
	setup(c)
	args := c.Args()
	for _, srcobj := range args {
		if srcbkt, srcbase, err := url2bktpath(s3cl, srcobj); err == nil {
			fmt.Printf("s3://%s/%s\n", srcbkt.Name, srcbase)
			if loc, err := srcbkt.Location(); err == nil {
				fmt.Println("Location:", loc)
			}
			vers, err := srcbkt.Versions(srcbase, "/", "", "", 1000)
			log.Println("vers", vers, err)
			if rd, err := srcbkt.GetReader(srcbase + "?acl"); err == nil {
				dec := xml.NewDecoder(rd)
				var acl aclpol
				dec.Decode(&acl)
				log.Printf("acl: %+v\n", acl)
			}
			if tr, err := srcbkt.Get(srcbase + "?torrent"); err == nil {
				log.Println("torrent", tr)
			}
		}
	}
}

func setacl(c *cli.Context) {
	setup(c)
}

func mv(c *cli.Context) {
	setup(c)
}

func sync(c *cli.Context) {
	setup(c)
	check_content := !c.Bool("size-only")
	do_del := c.Bool("delete")
	src := c.Args().Get(0)
	dst := c.Args().Get(1)
	_, _, srcerr := url2bktpath(s3cl, src)
	_, _, dsterr := url2bktpath(s3cl, dst)
	if srcerr == nil && dsterr != nil {
		syncfrom(src, dst, check_content, do_del)
	} else if srcerr != nil && dsterr == nil {
		syncto(dst, src, check_content, do_del)
	} else if srcerr == nil && dsterr == nil {
		syncremote(src, dst, check_content, do_del)
	} else {
		log.Fatal("src and dst are not s3 url ", src, dst)
	}
}

type entry struct {
	size    int64
	cksum   string
	lastmod time.Time
}

func filemd5(fn string) (string, error) {
	hs := md5.New()
	if fp, err := os.Open(fn); err != nil {
		return "", err
	} else {
		defer fp.Close()
		io.Copy(hs, fp)
		return hex.EncodeToString(hs.Sum(nil)), nil
	}
}

func listlocal(basedir string) map[string]entry {
	rst := map[string]entry{}
	filepath.Walk(basedir, func(pathname string, info os.FileInfo, err error) error {
		if !strings.HasPrefix(pathname, basedir) {
			log.Println("invalid path?", pathname, basedir)
			return err
		}
		pathname = strings.TrimPrefix(pathname, basedir)
		pathname = strings.TrimPrefix(pathname, "/")
		if info.Mode().IsRegular() {
			rst[pathname] = entry{size: info.Size(), lastmod: info.ModTime()}
		}
		return err
	})
	return rst
}

func lists3(s3url string, delimiter string) map[string]entry {
	rst := map[string]entry{}
	bkt, prefix, err := url2bktpath(s3cl, s3url)
	if err != nil {
		log.Fatal("invalid url:", err)
	}
	prefix = strings.TrimSuffix(prefix, delimiter)
	if prefix != "" {
		prefix = prefix + delimiter
	}
	var marker string
	for {
		rsp, err := bkt.List(prefix, "", marker, 1000)
		if err != nil {
			log.Println("error List", err)
			break
		}
		// log.Printf("list result: %+v", rsp)
		for _, k := range rsp.Contents {
			keystr := strings.TrimPrefix(k.Key, prefix)
			lm, _ := time.Parse("2006-01-02T15:04:05.000Z07:00", k.LastModified)
			rst[keystr] = entry{size: k.Size, cksum: strings.Trim(k.ETag, "\""), lastmod: lm}
		}
		marker = rsp.NextMarker
		if !rsp.IsTruncated {
			break
		}
	}
	return rst
}

func changelist(basedir string, src, dst map[string]entry, check_content bool) (to_update []string, to_del []string) {
	to_update = []string{}
	to_del = []string{}
	for k, s := range src {
		if d, ok := dst[k]; ok && s.size == d.size {
			if check_content {
				if s.cksum == "" {
					s.cksum, _ = filemd5(filepath.Join(basedir, k))
				} else if d.cksum == "" {
					d.cksum, _ = filemd5(filepath.Join(basedir, k))
				}
				if s.cksum == d.cksum {
					log.Println("md5 match", k, s.cksum)
					// pass
					continue
				} else {
					log.Println("md5 mismatch", k, s.cksum, d.cksum)
				}
			} else {
				continue
			}
		}
		to_update = append(to_update, k)
	}
	for k, _ := range dst {
		if _, ok := src[k]; ok {
			continue
		}
		to_del = append(to_del, k)
	}
	return
}

func syncto(s3url, basedir string, check_content, do_del bool) {
	// list localdir
	src := listlocal(basedir)
	log.Println("local", src)
	// list s3
	dst := lists3(s3url, "/")
	log.Println("s3", dst)
	to_update, to_del := changelist(basedir, src, dst, check_content)
	// put
	bkt, prefix, err := url2bktpath(s3cl, s3url)
	if err != nil {
		log.Println("url error", err)
	}
	log.Println("put", len(to_update), "files")
	for _, k := range to_update {
		dstname := filepath.Join(prefix, k)
		srcname := filepath.Join(basedir, k)
		log.Println("put", bkt.Name, srcname, dstname)
		st := time.Now()
		if ifp, err := os.Open(srcname); err == nil {
			fi, _ := ifp.Stat()
			bkt.PutReader(dstname, ifp, fi.Size(), "application/octet-stream", s3.Private, s3.Options{})
			ifp.Close()
			log.Println("finished", time.Since(st), fi.Size())
		} else {
			log.Println("open failed", err)
		}
	}
	if !do_del {
		return
	}
	// del
	log.Println("del", len(to_del), "objects")
	s3d := s3.Delete{}
	s3d.Quiet = true
	for _, k := range to_del {
		delname := filepath.Join(prefix, k)
		s3d.Objects = append(s3d.Objects, s3.Object{Key: delname})
		log.Println("del", bkt.Name, delname)
	}
	if len(s3d.Objects) != 0 {
		log.Println("emit del")
		bkt.DelMulti(s3d)
	}
}

func syncfrom(s3url, basedir string, check_content, do_del bool) {
	// list localdir
	dst := listlocal(basedir)
	log.Println("local", dst)
	// list s3
	src := lists3(s3url, "/")
	log.Println("s3", src)
	to_update, to_del := changelist(basedir, src, dst, check_content)
	// get
	log.Println("get", to_update)
	if !do_del {
		return
	}
	// unlink
	log.Println("unlink", to_del)
}

func syncremote(s3url_src, s3url_dst string, check_content, do_del bool) {
	// list s3_src
	src := lists3(s3url_src, "/")
	log.Println("s3src", src)
	// list s3_dst
	dst := lists3(s3url_dst, "/")
	log.Println("s3dst", dst)
	to_update, to_del := changelist("", src, dst, check_content)
	// putcopy
	log.Println("putcopy", to_update)
	if !do_del {
		return
	}
	// delete
	log.Println("del", to_del)
}

func setup(c *cli.Context) {
	var reg aws.Region
	var akey, skey string
	verbose = c.GlobalBool("verbose")
	if fp, err := os.Open(c.GlobalString("config")); err == nil {
		dec := json.NewDecoder(fp)
		var conf Config
		err = dec.Decode(&conf)
		if err != nil {
			log.Fatal("json decode %+v %v", conf, err)
		}
		if conf.Debug {
			verbose = true
		}
		akey = conf.AccessKey
		skey = conf.SecretKey
		reg.Name = "customized"
		reg.S3Endpoint = conf.StorageAPI
		if !conf.Force_path_style {
			u, _ := url.Parse(reg.S3Endpoint)
			u.Host = "${bucket}." + u.Host
			reg.S3BucketEndpoint = u.String()
			log.Println("not force_path_style:", reg.S3BucketEndpoint)
		}
		reg.S3LowercaseBucket = true
	} else if conf, err := ini.LoadFile(c.GlobalString("s3cfg")); err == nil {
		if v, ok := conf.Get("default", "verbosity"); ok && v == "DEBUG" {
			verbose = true
		}
		if v, ok := conf.Get("default", "bucket_location"); ok {
			reg = aws.Regions[v]
		} else {
			reg.Name = "customized"
			if reg.S3Endpoint, ok = conf.Get("default", "host_base"); ok {
				if ssl, ok := conf.Get("default", "use_https"); ok && ssl == "False" {
					reg.S3Endpoint = "http://" + reg.S3Endpoint + "/"
				} else {
					reg.S3Endpoint = "https://" + reg.S3Endpoint + "/"
				}
			}
			reg.S3LowercaseBucket = true
		}
		akey, _ = conf.Get("default", "access_key")
		skey, _ = conf.Get("default", "secret_key")
	} else {
		akey = c.GlobalString("access_key")
		skey = c.GlobalString("secret_key")
		if c.GlobalString("region") != "" {
			reg = aws.Regions[c.GlobalString("region")]
		}
		if c.GlobalString("endpoint") != "" {
			reg.Name = "customized"
			reg.S3Endpoint = c.GlobalString("endpoint")
		}
		if c.GlobalBool("force_path_style") {
			u, _ := url.Parse(reg.S3Endpoint)
			u.Host = "${bucket}." + u.Host
			reg.S3BucketEndpoint = u.String()
		}
	}
	if auth, err := aws.GetAuth(akey, skey, "", time.Now().Add(time.Hour)); err == nil {
		s3cl = s3.New(auth, reg)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	app := cli.NewApp()
	app.Name = "s3cmd"
	app.Usage = "AWS S3 API Client"
	app.Author = ""
	app.Email = ""
	app.Version = "0.1.0"
	var homedir string
	user, _ := user.Current()
	if user == nil {
		homedir = os.Getenv("HOME")
	} else {
		homedir = user.HomeDir
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: path.Join(homedir, ".dagrin", "credential.json"),
			Usage: "config file",
		},
		cli.StringFlag{
			Name:  "s3cfg, s",
			Value: path.Join(homedir, ".s3cfg"),
			Usage: "s3cmd config file",
		},
		cli.StringFlag{
			Name:   "access_key",
			Usage:  "Access Key ID",
			EnvVar: "AWS_ACCESS_KEY,AWS_ACCESS_KEY_ID",
		},
		cli.StringFlag{
			Name:   "secret_key",
			Usage:  "Secret Access Key",
			EnvVar: "AWS_SECRET_ACCESS_KEY,AWS_SECRET_KEY",
		},
		cli.StringFlag{
			Name:   "endpoint",
			Usage:  "Endpoint",
			EnvVar: "AWS_S3_ENDPOINT",
		},
		cli.StringFlag{
			Name:   "region",
			Usage:  "AWS Region",
			EnvVar: "AWS_REGION,AWS_S3_REGION",
		},
		cli.BoolFlag{
			Name:  "verbose",
			Usage: "Verbose Message",
		},
		cli.BoolFlag{
			Name:  "progress",
			Usage: "Show Progress Bar",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:      "list",
			ShortName: "ls",
			Usage:     "list objects or buckets",
			Action:    ls,
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "long,l",
					Usage: "use long listing format",
				},
				cli.BoolFlag{
					Name: "recursive,R",
				},
			},
		}, {
			Name:      "list-all",
			ShortName: "la",
			Usage:     "list all object in all buckets",
			Action:    la,
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "long,l",
					Usage: "use long listing format",
				},
			},
		}, {
			Name:      "put",
			ShortName: "write",
			Usage:     "put file into bucket",
			Action:    put,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "content-type,t",
					Value: "binary/octet-stream",
					Usage: "set content type",
				},
			},
		}, {
			Name:      "get",
			ShortName: "read",
			Usage:     "get file from bucket",
			Action:    get,
		}, {
			Name:      "cat",
			ShortName: "dd",
			Usage:     "read file from bucket",
			Action:    cat,
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name: "recursive,R",
				},
			},
		}, {
			Name:      "getrange",
			ShortName: "readrange",
			Usage:     "get file from bucket",
			Action:    catrange,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "range",
					Usage: "range str",
				},
			},
		}, {
			Name:   "du",
			Usage:  "du bucket",
			Action: du,
		}, {
			Name:   "da",
			Usage:  "du all bucket",
			Action: da,
		}, {
			Name:      "del",
			ShortName: "rm",
			Usage:     "delete object",
			Action:    del,
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name: "recursive,R",
				},
			},
		}, {
			Name:      "copy",
			ShortName: "cp",
			Usage:     "copy object",
			Action:    cp,
		}, {
			Name:      "putmulti",
			ShortName: "pm",
			Usage:     "put object with multipart upload",
			Action:    putmulti,
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "split",
					Value: 16 * 1024 * 1024,
					Usage: "split size",
				},
				cli.StringFlag{
					Name:  "content-type,t",
					Value: "binary/octet-stream",
					Usage: "set content type",
				},
			},
		}, {
			Name:      "merge",
			ShortName: "join",
			Usage:     "merge large objects",
			Action:    merge,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "content-type,t",
					Value: "binary/octet-stream",
					Usage: "set content type",
				},
				cli.BoolFlag{
					Name:  "dry-run,n",
					Usage: "do not merge",
				},
			},
		}, {
			Name:      "makebucket",
			ShortName: "mb",
			Usage:     "make bucket",
			Action:    mb,
		}, {
			Name:      "removebucket",
			ShortName: "rb",
			Usage:     "remove bucket",
			Action:    rb,
		}, {
			Name:   "exists",
			Usage:  "check object exists",
			Action: exists,
		}, {
			Name:   "head",
			Usage:  "dump header",
			Action: head,
		}, {
			Name:      "listmulti",
			ShortName: "lm",
			Usage:     "list ongoing multipart upload",
			Action:    listmulti,
		}, {
			Name:   "info",
			Usage:  "get info",
			Action: info,
		}, {
			Name:   "setacl",
			Usage:  "set acl",
			Action: setacl,
		}, {
			Name:   "mv",
			Usage:  "move object",
			Action: mv,
		}, {
			Name:   "sync",
			Usage:  "sync directory tree",
			Action: sync,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "content-type,t",
					Value: "binary/octet-stream",
					Usage: "set default content type",
				},
				cli.IntFlag{
					Name:  "split",
					Value: 0,
					Usage: "do multipart upload",
				},
				cli.BoolFlag{
					Name:  "delete,d",
					Usage: "delete file/objects when deleted from src",
				},
				cli.BoolFlag{
					Name:  "size-only,s",
					Usage: "compare only size",
				},
			},
		},
	}
	app.Run(os.Args)
}
