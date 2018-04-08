package elastic

import (
	"testing"
)

func TestCURD(t *testing.T) {
	createEmptyUsers()

	if err := testES.Put(`/`, nil, nil); !IsIndexAreadyExists(err) {
		t.Errorf("expect ErrorIndexAreadyExists, got: %v", err)
	}

	testES.Create(`/_doc/1`, map[string]interface{}{`name`: `lilei`, `age`: 21}, nil)
	testES.Create(`/_doc/2`, map[string]interface{}{`name`: `hanmeimei`, `age`: 19}, nil)
	testES.Create(`/_doc/3`, map[string]interface{}{`name`: `tom`, `age`: 22}, nil)

	testES.Delete(`/_doc/3`, nil)

	testES.Update(`/_doc/1`, map[string]map[string]int{`doc`: {`age`: 31}}, nil)
	testES.Update(`/_doc/2`, map[string]map[string]int{`doc`: {`age`: 29}}, nil)

	checkLiLeiAndHanMeiMei(t)
}
