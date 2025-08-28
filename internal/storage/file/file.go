package file

import "os"

/**
* This module is used to read and write data from / to disk
* we will map the file to memory in disk that facilitate accessility to disk
**/
type FileManager struct {
	file *os.File
}

// When read from disk -> Deseialize the data to page.Page
// When write to disk -> Serialize the data to []byte and store them in disk by offset
