package document

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/blugelabs/bluge"
)

func loadData(writer *bluge.Writer) error {
	datas := []map[string]interface{}{
		{"_id": "a", "category": "inventory", "type": "book", "updated": time.Now(), "rating": 1},
		{"_id": "b", "category": "inventory", "type": "book", "updated": time.Now(), "rating": 2},
		{"_id": "c", "category": "tech", "type": "book", "updated": time.Now(), "rating": 3},
		{"_id": "d", "category": "tech", "type": "book", "updated": time.Now(), "rating": 4},
		{"_id": "e", "category": "social", "type": "book", "updated": time.Now(), "rating": 5},
		{"_id": "f", "category": "social", "type": "movie", "updated": time.Now(), "rating": 6},
		{"_id": "g", "category": "social", "type": "movie", "updated": time.Now(), "rating": 7},
		{"_id": "h", "category": "inventory", "type": "movie", "updated": time.Now(), "rating": 8},
		{"_id": "i", "category": "inventory", "type": "game", "updated": time.Now(), "rating": 9},
	}
	for i := 0; i < 100; i++ {
		for _, row := range datas {
			doc := bluge.NewDocument(fmt.Sprintf("%s%d", row["_id"], i))
			for k, v := range row {
				if k == "_id" || k == "_source" || k == "_all" {
					continue
				}
				switch v := v.(type) {
				case string:
					doc.AddField(bluge.NewTextField(k, v).Aggregatable())
				case int:
					doc.AddField(bluge.NewNumericField(k, float64(v)).Aggregatable())
				case int32:
					doc.AddField(bluge.NewNumericField(k, float64(v)).Aggregatable())
				case int64:
					doc.AddField(bluge.NewNumericField(k, float64(v)).Aggregatable())
				case float32:
					doc.AddField(bluge.NewNumericField(k, float64(v)).Aggregatable())
				case float64:
					doc.AddField(bluge.NewNumericField(k, float64(v)).Aggregatable())
				case time.Time:
					doc.AddField(bluge.NewDateTimeField(k, v).Aggregatable())
				default:
					return fmt.Errorf("not support value type: %T", v)
				}
			}
			source, _ := json.Marshal(row)
			doc.AddField(bluge.NewStoredOnlyField("_source", source))
			doc.AddField(bluge.NewCompositeFieldExcluding("_all", []string{"_id"}))
			if err := writer.Insert(doc); err != nil {
				return err
			}
		}
	}

	return nil
}
