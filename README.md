<div id="top"></div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

# Sparkify

A project from the Data Engineering Nanodegree Program to practice data modeling in relational databases using PostgreSQL.

## About The Project

...

## Getting Started

### Prerequisites

...

### Installation

...


### Using a conda environment

#### Initialize PostgreSQL database

##### Create base database

```bash
initdb -D base_db
```

##### Start postgress server

```bash
pg_ctl -D base_db -l logfile start
```

##### Create super user

```bash
createuser -s --encrypted --pwprompt student
```

##### Create student database

```bash
createdb --owner=student studentdb
```

#### Something something...


### Using a docker image

...

## Usage

...

Code formatted with "isort .; autoflake -r --in-place --remove-unused-variables --remove-all-unused-imports --ignore-init-module-imports .; black ."

<p align="right">(<a href="#top">back to top</a>)</p>


<!-- LICENSE -->

## License

Distributed under the MIT License. See `LICENSE` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- CONTACT -->

## Contact

[Carlos Uziel Pérez Malla](https://www.carlosuziel-pm.dev/)

[GitHub](https://github.com/CarlosUziel) - [Google Scholar](https://scholar.google.es/citations?user=tEz_OeIAAAAJ&hl=es&oi=ao) - [LinkedIn](https://at.linkedin.com/in/carlos-uziel-p%C3%A9rez-malla-323aa5124) - [Twitter](https://twitter.com/perez_malla)

<p align="right">(<a href="#top">back to top</a>)</p>

## Acknowledgments

...

<p align="right">(<a href="#top">back to top</a>)</p>