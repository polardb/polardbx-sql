#! /bin/sh

drds_path='/home/admin/drds-server'
cleaner_log='/home/admin/drds-server/logs/cleaner.log'

if [ -z $1 ]; then
    max_used=90
else
    max_used=$1
fi

cd $drds_path

print() {
    if [ -z $2 ]; then
        echo $1 >> $cleaner_log
    else
        echo "level[$2] $1" >> $cleaner_log
    fi
}


if [ -z "`ls -A $drds_path`" ]; then
   print "$drds_path is empty!"
   exit
else
   print "handle: $drds_path"
fi

get_disc_space() {
    raw_amount=`df -h $drds_path|sed '1d'`
    file_system=`echo $raw_amount|awk '{print $1}'`
    use=`echo $raw_amount|awk '{print $5+0}'`
    mnt=`echo $raw_amount|awk '{print $6}'`
    return $use
}


clean_process_log() {
    current_process_path=$1
    if [ -z "`ls -A $current_process_path`" ]; then
        return
    else
        print "handle: $current_process_path" $2
    fi

    ls -lhtr $current_process_path|sed '1d'|while read LINE
    do
        db_path="$current_process_path/`echo $LINE|awk '{print $9}'`"
        clean_db_log $db_path $2

	get_disc_space
        if [ $use -lt $max_used ]; then
            return $use
        fi
    done
}

clean_db_log() {
    if [ -z "`ls -A $1`" ]; then
        return $use
    else
        print "handle: $1" $2
    fi

    if [ -z "`ls -hl $1|sed '1d'|sed '/.\.log/d'|sed '/.\.db/d'`" ]; then

        print "no folder remained, clear *.log" $2

        ls -hl $1|sed '1d'|sed '/.\.db/d'|sed '/txc.*\.\(.*\.\)*log\.1$/d'|sed '/txc.*\.\(.*\.\)*log$/d'| awk '{print $9}'|while read LINE
        do
            log_path="$1/$LINE"
            clean_log $log_path $2

            if [ $use -lt $max_used ]; then
                return $use
            fi
        done
    else

        skip_count=`expr 5 - $2 \* 2`
        if [ $skip_count -lt $[0] ]; then
            skip_count=0
        fi

        total_folder_count=`ls -lhtr $1|sed '1d'|sed '/.\.log/d'|sed '/.\.db/d'|wc -l`
        if [ $skip_count -gt $total_folder_count ]; then
            print "no folder removed in[$1]" $2
            return $use
        fi

        print "remove folders first" $2

        print "skip latest $skip_count folders" $2

        if [ $skip_count -gt $[0] ]; then
            ls -lhtr $1|sed '1d'|sed '/.\.log/d'|sed '/.\.db/d'|awk '{print $9}'|sort -r -t- -k1,1 -k2,2 -k3,3|sed "1, $skip_count d"|sort -r -t- -k1,1 -k2,2 -k3,3|while read LINE
            do
                log_path="$1/$LINE"
                clean_log $log_path $2
            done
        else
            ls -lhtr $1|sed '1d'|sed '/.\.log/d'|sed '/.\.db/d'|awk '{print $9}'|sort -t- -k1,1 -k2,2 -k3,3|while read LINE
            do
                log_path="$1/$LINE"
                clean_log $log_path $2
            done
        fi
    fi
}

clean_log() {
    clean_log_path=`echo $1|sed 's/ /\\\ /g'`

    if [ -d $clean_log_path ]; then
        print "dir[$clean_log_path] removed!" $2
        rm -rvf $clean_log_path
    else
        print "file[$clean_log_path] cleared!" $2
        cat /dev/null >  $clean_log_path
    fi

    get_disc_space
    return $use
}



print "clean start! date[`date '+%Y/%m/%d %T'`]===================="
# 0：删除5天以前的日志文件夹；1：删除3天以前的日志文件夹；2：删除1天前的日志文件夹；3：删除所有历史日志；4：删除所有日志
for clean_level in 0 1 2 3 4
do
    get_disc_space

    if [ $use -ge $max_used ];then
        print "path[$drds_path] file system[$file_system] mount on[$mnt] used[$use%]"

        eagleeye_log_path="/home/admin/logs/eagleeye"
        clean_db_log $eagleeye_log_path $clean_level

        ls -hl $drds_path|sed '1d'|sed '/bin/d'|sed '/conf/d'|sed '/lib/d'|sed '/logs/d'|while read LINE
        do
            process_path="$drds_path/`echo $LINE|awk '{print $9}'`/logs"
            clean_process_log $process_path $clean_level
        done

        txc_log_path="/home/admin/logs/txc"
        clean_db_log $txc_log_path $clean_level
    else
        print "clean finish! current usage[$use%] `date`===================="
        exit
    fi
done

if [ $use -ge $max_used ];then
    clean_log $cleaner_log
fi

use=`df -h $drds_path|sed '1d'|awk '{print $5+0}'`
print "clean finish! current usage[$use%] `date`===================="

