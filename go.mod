module github.com/pasdam/go-csv-timeseries-db

replace github.com/pasdam/go-csv-timeseries-db/pkg => ./pkg

go 1.17

require (
	github.com/pasdam/go-files-test v0.0.0-20200523130716-5dc6c4313161
	github.com/pasdam/go-io-utilx v0.0.0-20201229215823-570b5ea4df86
	github.com/pasdam/go-search v0.0.0-20201229215808-279b97b7d69a
	github.com/pasdam/mockit v0.0.0-20240524154541-fb54ecfd0e1e
	github.com/stretchr/testify v1.11.1
)

require (
	bou.ke/monkey v1.0.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
