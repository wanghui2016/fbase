package tablet

//a tablet = an in-memory array from rowID to offset + an AOF of row mutations
//a row mutation = [last & current version of column]

type Tablet struct{}

func NewTablet(startRowID int64, capx int64) *Tablet {
	return nil
}

func (t *Tablet) Insert(row []byte) (rowID int64, e error) {}

func (t *Tablet) Delete(rowID int64) error {}

func (t *Tablet) Update(rowID int64, updatedColumns []byte) error {}
