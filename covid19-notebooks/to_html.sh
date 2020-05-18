#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
mkdir -p $DIR/html-notebooks

function convert_to_html {
    echo Converting to HTML: $1
    jupyter nbconvert --to html $DIR/$1 --output-dir $DIR/html-notebooks
}

convert_to_html "chicago-seir-forecast/covid19_seir.ipynb"
convert_to_html "jhu-summary-overview/COVID-19-JHU_data_analysis_04072020.ipynb"
convert_to_html "kaggle-demographics/kaggle_data_analysis_04072020.ipynb"
convert_to_html "extended-seir/extended-seir.ipynb"
