package indexlet

//a sorted map from keys to posting lists of rowIDs

type Indexlet struct{}

func CreateIndexlet(startKey, endKey []byte) *Indexlet {}

func (i *Indexlet) Insert(key []byte, rowID int64) error {}

func (i *Indexlet) Delete(key []byte, rowID int64) error {}
