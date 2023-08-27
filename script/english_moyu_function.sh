#!/bin/bash
function get_moyu_log_date_str() {
    ds_date=$1
    year=${ds_date: 0: 4}
    month_flag=${ds_date: 4: 1}

    if [ $month_flag = "1" ]
    then
        month=${ds_date: 4: 2}
    else
        month=${ds_date: 5: 1}
    fi

    day_flag=${ds_date: 6: 1}
    if [ $day_flag = "0" ]
    then
        day=${ds_date: 7: 1}
    else
        day=${ds_date: 6: 2}
    fi

    echo "$year-$month-$day"
}