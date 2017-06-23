# NHS GP Crawler

A simple web crawler to try to rank NHS GPs. This uses [scrapy](https://scrapy.org/) under the hood

## install
    pip install -r requirements.tt

## running
    scrapy runspider nhsgpspider.py -L WARN -t csv -o - -a postcode=N19PH | tee results
