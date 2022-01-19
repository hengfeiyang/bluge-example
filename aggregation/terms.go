package aggregation

import (
	"context"
	"fmt"
	"log"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	"github.com/blugelabs/bluge/search/aggregations"
	"github.com/safeie/bluge-example/aggregation/custom"
)

func terms() {
	cfg := bluge.DefaultConfig("../data/myindex")
	writer, err := bluge.OpenWriter(cfg)
	if err != nil {
		log.Fatal(err)
	}
	// err = loadData(writer)
	// err = searchQuery(writer)
	err = searchAggs(writer)
	if err != nil {
		log.Fatal(err)
	}

	writer.Close()
}

func searchAggs(writer *bluge.Writer) error {
	reader, err := writer.Reader()
	if err != nil {
		return err
	}
	query := bluge.NewBooleanQuery().AddMust(bluge.NewMatchAllQuery())
	searchRequest := bluge.NewTopNSearch(1, query).WithStandardAggregations()
	termAgg := custom.NewTermsAggregation(search.Field("type"), custom.TextValueSource, 100)
	termAgg.AddAggregation("sub-term1", custom.NewTermsAggregation(search.Field("type"), custom.TextValueSource, 100))
	termAgg.AddAggregation("sub-term2", custom.NewTermsAggregation(search.Field("category"), custom.TextValueSource, 100))
	termAgg.AddAggregation("sub-term3", custom.NewTermsAggregation(search.Field("rating"), custom.NumericValuesSource, 100))
	searchRequest.AddAggregation("my-agg-term", termAgg)
	searchRequest.AddAggregation("my-agg-card", aggregations.Cardinality(search.Field("category")))
	searchRequest.AddAggregation("my-agg-max", aggregations.Max(search.Field("rating")))
	searchRequest.AddAggregation("my-agg-min", aggregations.Min(search.Field("rating")))
	searchRequest.AddAggregation("my-agg-avg", aggregations.Avg(search.Field("rating")))
	searchRequest.AddAggregation("my-agg-sum", aggregations.Sum(search.Field("rating")))
	dmi, err := reader.Search(context.Background(), searchRequest)
	if err != nil {
		return err
	}

	bucket := dmi.Aggregations()
	aggs := bucket.Aggregations()
	for k, v := range aggs {
		switch v := v.(type) {
		case search.MetricCalculator:
			fmt.Printf("%s, %.2f\n", k, v.Value())
		case search.DurationCalculator:
			fmt.Printf("%s, %d\n", k, v.Duration().Milliseconds())
		case search.BucketCalculator:
			fmt.Printf("%s ->\n", k)
			buckets := v.Buckets()
			for i, bucket := range buckets {
				fmt.Println(i, bucket.Name(), bucket.Count())
				for ks, vs := range bucket.Aggregations() {
					switch v := vs.(type) {
					case search.MetricCalculator:
						fmt.Printf("sub %s, %.2f\n", ks, v.Value())
					case search.BucketCalculator:
						fmt.Printf("sub %s ->\n", k)
						buckets := v.Buckets()
						for j, bucket := range buckets {
							fmt.Println("sub->sub", i, ks, j, bucket.Name(), bucket.Count())
						}
					}
				}
			}
		default:
			fmt.Printf("name: %s, type: %T\n", k, v)
		}
	}

	total := dmi.Aggregations().Count()
	times := dmi.Aggregations().Duration().Milliseconds()
	fmt.Printf("total: %d, times: %d, err: %v\n\n", total, times, err)

	return nil

}
