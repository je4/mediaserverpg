package service

import "time"

type item struct {
	Id            string
	Signature     string
	Collectionid  string
	Urn           string
	Type          string
	Subtype       string
	Mimetype      string
	Error         string
	Sha512        string
	Metadata      string
	CreationDate  time.Time
	LastModified  time.Time
	Disabled      bool
	Public        bool
	PublicActions string
	Status        string
	PartentId     string
	Objecttype    string
}
