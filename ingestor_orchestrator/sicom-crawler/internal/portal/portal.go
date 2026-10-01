// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package portal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/crawler"
	sicomidentity "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const APIURL = "https://arabiasaudita.tce.mg.gov.br:8443/TCEMG-proxy-web/publico/wso2amgw/dados-abertos/dadosAbertos/"

type Session struct {
	ProxyToken, Bearer string
	Years              []int
}
type Authenticate func(context.Context) (Session, error)
type Client struct {
	HTTP         *http.Client
	BaseURL      string
	Authenticate Authenticate
	session      Session
}

var yearList = regexp.MustCompile(`(?s)\.listaExercicio\s*=\s*\[([^\]]+)\]`)
var yearValue = regexp.MustCompile(`descExercicio\s*:\s*["']([0-9]{4})["']`)
var bearerValue = regexp.MustCompile(`bearer\s*:\s*["']([^"']+)["']`)

func ParseYears(script string) ([]int, error) {
	list := yearList.FindStringSubmatch(script)
	if len(list) != 2 {
		return nil, errors.New("SICOM year list absent in portal scripts")
	}
	years := []int{}
	seen := map[int]bool{}
	for _, match := range yearValue.FindAllStringSubmatch(list[1], -1) {
		y, _ := strconv.Atoi(match[1])
		if !seen[y] {
			years = append(years, y)
			seen[y] = true
		}
	}
	if len(years) == 0 {
		return nil, errors.New("SICOM year list is empty")
	}
	return years, nil
}
func ParseBearer(script string) string {
	m := bearerValue.FindStringSubmatch(script)
	if len(m) == 2 {
		return m[1]
	}
	return ""
}
func (c *Client) refresh(ctx context.Context) error {
	session, err := c.Authenticate(ctx)
	if err != nil {
		return err
	}
	if session.ProxyToken == "" || session.Bearer == "" || len(session.Years) == 0 {
		return errors.New("portal session missing credentials or years")
	}
	c.session = session
	return nil
}
func (c *Client) Years(ctx context.Context) ([]int, error) {
	if err := c.refresh(ctx); err != nil {
		return nil, err
	}
	return c.session.Years, nil
}
func (c *Client) request(ctx context.Context, target string) (*http.Response, error) {
	if c.session.ProxyToken == "" {
		if err := c.refresh(ctx); err != nil {
			return nil, err
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, errors.New("invalid portal API URL")
		}
		req.Header.Set("Authorization", "Bearer "+c.session.Bearer)
		token := c.session.ProxyToken
		if !strings.HasPrefix(token, "token ") {
			token = "token " + token
		}
		req.Header.Set("AuthorizationProxy", token)
		req.Header.Set("Accept", "application/json, text/plain, */*")
		req.Header.Set("Origin", strings.TrimRight(sicomidentity.PortalURL, "/"))
		req.Header.Set("Referer", sicomidentity.PortalURL)
		res, err := c.HTTP.Do(req)
		if err != nil {
			return nil, errors.New("portal API transport failed")
		}
		if (res.StatusCode == 401 || res.StatusCode == 403) && attempt == 0 {
			_ = res.Body.Close()
			if err := c.refresh(ctx); err != nil {
				return nil, err
			}
			continue
		}
		if res.StatusCode != http.StatusOK {
			_ = res.Body.Close()
			return nil, fmt.Errorf("portal API HTTP %d", res.StatusCode)
		}
		// The proxy can rotate a request token after an authenticated response.
		if token := res.Header.Get("AuthorizationProxy"); token != "" {
			c.session.ProxyToken = token
		}
		return res, nil
	}
	return nil, errors.New("portal authorization failed after renewal")
}
func (c *Client) base() string {
	if c.BaseURL != "" {
		return c.BaseURL
	}
	return APIURL
}
func (c *Client) List(ctx context.Context, year int) ([]crawler.Category, error) {
	query := url.Values{"exercicio": {strconv.Itoa(year)}, "origem": {"SICOM"}}
	res, err := c.request(ctx, c.base()+"buscarCategoriaDownload?"+query.Encode())
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	categories := []crawler.Category{}
	if err := json.NewDecoder(io.LimitReader(res.Body, 8<<20)).Decode(&categories); err != nil {
		return nil, errors.New("invalid portal category response")
	}
	for i := range categories {
		if categories[i].SeqZip <= 0 || categories[i].Name == "" {
			return nil, errors.New("invalid ZIP identity in category response")
		}
		categories[i].Year = year
	}
	return categories, nil
}
func (c *Client) Download(ctx context.Context, category crawler.Category) (io.ReadCloser, error) {
	res, err := c.request(ctx, c.DownloadURL(category))
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}
func (c *Client) DownloadURL(category crawler.Category) string {
	return c.base() + "baixarArquivoPct/" + strconv.FormatInt(category.SeqZip, 10)
}
