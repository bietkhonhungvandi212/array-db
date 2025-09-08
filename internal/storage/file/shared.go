package file

import (
	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	utils "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

type Filer interface {
	ReadPage(pageId utils.PageID) (*page.Page, error)
	WritePage(p *page.Page) error
}
