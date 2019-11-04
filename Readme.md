## OrgSynScraper

This repository contains a Python script that lets you scrape all PDF links
from the website [http://orgsyn.org/][1] and download the PDF files.

A little bit of background information about the project is available in
[this post on my blog][2].

### Example usage:

Dumping only the links of a specific volume, for example volume 42:

```
./org_syn_scraper.py dump_links --volume=42 --links-only
```

Dumping the links and additional information of a specific volume:

```
./org_syn_scraper.py dump_links --volume=60
```

This returns an JSON array of objects with the following keys:

| Key             | Description                                                                      |
|-----------------|----------------------------------------------------------------------------------|
| `annual_volume` | The annual volume containing the document                                        |
| `page`          | The page of the document in the annual volume                                    |
| `name`          | The name of the procedure described by the document                              |
| `aliases`       | An array with alternative names of the procedure described by the document       |
| `slug`          | A slug generated out of the name of the procedure, that can be used as file name |
| `url`           | The URL of the PDF document                                                      |

Downloading all files into the directory `output`:

```
./org_syn_scraper.py download output
```

Downloading all files of volume 96 into the directory `volume_96`:

```
./org_syn_scraper.py download --volume=96 output
```


  [1]: http://orgsyn.org/
  [2]: https://blog.kalehmann.de/blog/2019/11/03/orgsyn-scraper.html
