package query

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	"github.com/blugelabs/bluge/search/highlight"
)

func searchQuery(writer *bluge.Writer) error {
	reader, err := writer.Reader()
	if err != nil {
		return err
	}
	query := bluge.NewBooleanQuery().AddMust(bluge.NewMatchAllQuery())
	searchRequest := bluge.NewTopNSearch(10, query).WithStandardAggregations()
	dmi, err := reader.Search(context.Background(), searchRequest)
	if err != nil {
		return err
	}

	matches, err := collectHits(dmi)
	if err != nil {
		return err
	}

	total := dmi.Aggregations().Count()
	times := dmi.Aggregations().Duration().Milliseconds()
	body, err := json.Marshal(matches)
	fmt.Printf("total: %d, times: %d, err: %v\n%s\n", total, times, err, body)

	return nil
}

func collectHits(dmi search.DocumentMatchIterator) (rv []*match, err error) {
	var next *search.DocumentMatch
	next, err = dmi.Next()
	for next != nil && err == nil {
		nextMatch := &match{
			Number:    next.Number,
			Score:     next.Score,
			Fields:    map[string]interface{}{},
			Locations: next.Locations,
		}
		err = next.VisitStoredFields(func(field string, value []byte) bool {
			if field == "_source" {
				var result map[string]interface{}
				json.Unmarshal(value, &result)
				nextMatch.Fields[field] = result
				return true
			}
			cp := make([]byte, len(value))
			copy(cp, value)
			nextMatch.Fields[field] = cp
			return true
		})
		if err != nil {
			return nil, fmt.Errorf("error visiting stored fields: %v", err)
		}
		rv = append(rv, nextMatch)
		next, err = dmi.Next()
	}
	if err != nil {
		return nil, fmt.Errorf("error iterating results:  %v", err)
	}
	return rv, nil
}

type match struct {
	Number           uint64
	Score            float64
	SortValue        [][]byte
	Fields           map[string]interface{}
	ExpectHighlights []*ExpectHighlight
	Locations        search.FieldTermLocationMap
}

type ExpectHighlight struct {
	Highlighter highlight.Highlighter
	Field       string
	Result      string
}
