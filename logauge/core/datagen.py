"""
Utility library to generate a dataset using a source/seed file.

Splunk Performance Team, 2013 (eng-perf@splunk.com)
"""
import os
import time
import calendar
from datetime import datetime, tzinfo, timedelta

from common import next_term

class UTC(tzinfo):
    """UTC"""
    def utcoffset(self, dt):
        return timedelta(0)
    def tzname(self, dt):
        return "UTC"
    def dst(self, dt):
        return timedelta(0)

def generate_custom(
        input_path, output_path, filesize_gb=0, events=0, num_files=1,
        datarate_gbph=0, runtime_s=0, overwrite=True, timestamp_token='@TIME@',
        timestamp_format='%b %d %Y %H:%M:%S %Z', field_token='@FIELD@', logger=None,
        files_to_keep=1, globalNum=1):
    """Generates data with the given parameters.

    Args:
        input_path (str): path to a sample data file of any size.  This input
            file can contain markers for timestamps and extra fields
        output_path (str): path to the output file to generate
        filesize_gb (float): max size of the output file.  0 for infinite.
        events (int): max # of distinct events to generate.  0 for infinite
        num_files (int): number of different files to generate.  Each file will
            be subject to the size, rate, and event limits separately.  If set
            to 0, data generator will loop indefinitely while creating new
            files each loop.  1 by default.
        datarate_gbph (float): max data rate in GB/hr to generate with.  0 for
            unlimited, or as fast as possible.
        runtime_s (int): max runtime of the data generator in seconds.  Also
            governs the generation of multiple files.
        overwrite (bool): if True, overwrite the output file before starting.
            If False, simply append to an existing file.    True by default.
        timestamp_token (str): string in the source file that will be replaced
            with timestamps by the data generator.    Default is '@TIME@'
        timestamp_format (str): string representing the format of the generated
            timestamps.  Default is '%b %d %Y %H:%M:%S %Z'
        field_token (str): string in the source file that will be replaced with
            extra fields by the data generator.    Default is '@FIELD@'
        logger (logging.Logger): optional python logger object that will be
            used to report progress and resulting event counts
        files_to_keep (int): Maximum files to keep on hand, if generating
            multiple files

    Returns:
        Number of events generated for the entire function call (integer)

    EXAMPLE SOURCE LINE:
    @TIME@ [165.228.86.151] @FIELD@ wms[493]: <DBUG> mysql: UPDATE probe_sta_table SET signal='31', noise='0', frame_retry_rate='0', frame_low_speed_rate='100', frame_receive_error_rate='0', bandwidth_rate='2', modified_time=NOW() WHERE probe_id='153' AND sta_id='360'
    """
    utc = UTC()
    # 10 events per second starting from timeSubtract
    #timeSubtract = 365 * 24 * 60 * 60
    timeSubtract = 0
    bytesPerSecondMax = (datarate_gbph * 1024 * 1024 * 1024) / 3600.0
    maxBytes = filesize_gb * 1024 * 1024 * 1024
    i = 0
    startTime = time.time()
    all_files = []
    while True:
        if (num_files != 0) and (i >= num_files):
            break
        elif (runtime_s != 0) and ((time.time() - startTime) >= runtime_s):
            break
            # Check if old files need to be deleted, and keep deleting them until we have room
        while len(all_files) >= files_to_keep:
            old_file = all_files.pop(0)
            if (logger is not None):
                logger.info(
                    'Removing old data file %s to make room for new '
                    'ones' % old_file)
            if os.path.exists(old_file):
                os.remove(old_file)

        if num_files != 1:
            outFile_path = output_path.replace('.', '_%d.' % (i+1), 1)
        else:
            outFile_path = output_path
        if overwrite:
            if (logger is not None):
                if os.path.exists(outFile_path):
                    logger.info('Overwriting %s' % outFile_path)
                else:
                    logger.info(
                        'Nothing to overwrite. Creating new file '
                        '%s' % outFile_path)
            f = open(outFile_path, 'w')
            f.close()
        else:
            if (logger is not None):
                if os.path.exists(outFile_path):
                    logger.info('Appending %s' % outFile_path)
                else:
                    logger.info('Nothing to append. Creating new file '
                                '%s' % outFile_path)
            f = open(outFile_path, 'a')
            f.close()
        all_files.append(outFile_path)
        bytesWritten = 0
        bytesThisSecond = 0
        count = 0
        lineNum = 1
        lastSleep = time.clock()
        last_log_event = time.time()
        t = datetime.utcnow()
        # subtract secs in day from current epoch time
        newStartTime = datetime.fromtimestamp(
            calendar.timegm(t.utctimetuple()) - timeSubtract, utc)

        while(True):
            timeElapsed = time.time() - startTime
            if ( ((runtime_s != 0) and (timeElapsed >= runtime_s)) or
                     ((events != 0) and (lineNum > events)) or
                     ((filesize_gb != 0) and (bytesWritten >= maxBytes))):
                break
            outFile = open(outFile_path, 'a', 0)
            inFile = open(input_path, 'rU', 0)
            inFile.seek(0)
            for inLine in inFile:
                timeElapsed = time.time() - startTime
                if (((runtime_s != 0) and (timeElapsed >= runtime_s)) or
                        ((events != 0) and (lineNum > events)) or
                        ((filesize_gb != 0) and (bytesWritten >= maxBytes))):
                    break
                count+=1
                if (count > 10):
                    newStartTime = datetime.fromtimestamp(
                        calendar.timegm(newStartTime.utctimetuple()) + 1, utc)
                    count = 1
                timeElapsed = time.time() - startTime
                if ( ((runtime_s != 0) and (timeElapsed >= runtime_s)) or
                         ((events != 0) and (lineNum > events)) or
                         ((filesize_gb != 0) and (bytesWritten >= maxBytes))):
                    break


                my_custom_marker = next_term(globalNum, 10) \
                                   + ' ' + next_term(globalNum, 9) \
                                   + ' ' + next_term(globalNum, 8) \
                                   + ' ' + next_term(globalNum, 7) \
                                   + ' ' + next_term(globalNum, 6) \
                                   + ' ' + next_term(globalNum, 5) \
                                   + ' ' + next_term(globalNum, 4) \
                                   + ' ' + next_term(globalNum, 3) \
                                   + ' ' + next_term(globalNum, 2) \
                                   + ' ' + next_term(globalNum, 1)

                #markers = 'every1 '
                markers = 'eventnum=' + str(globalNum) + ' ' + my_custom_marker + ' every1 '

                if (globalNum % 10) == 0:
                    markers += 'every10 '
                if (globalNum % 100) == 0:
                    markers += 'every100 '
                if (globalNum % 1000) == 0:
                    markers += 'every1K '
                if (globalNum % 10000) == 0:
                    markers += 'every10K '
                if (globalNum % 100000) == 0:
                    markers += 'every100K '
                if (globalNum % 1000000) == 0:
                    markers += 'every1M '
                if (globalNum % 10000000) == 0:
                    markers += 'every10M '
                if (globalNum % 100000000) == 0:
                    markers += 'every100M '
                if (globalNum % 1000000000) == 0:
                    markers += 'every1B '
                if (globalNum % 10000000000) == 0:
                    markers += 'every10B '


                if field_token is not None and field_token in inLine:
                    inLineMarked = inLine.replace(field_token, markers)
                else:
                    inLineMarked = markers + inLine

                if (datarate_gbph != 0):
                    timestamp = datetime.now(utc).strftime(timestamp_format)
                else:
                    timestamp = newStartTime.strftime(timestamp_format)

                if timestamp_token is not None and timestamp_token in inLineMarked:
                    outLine = inLineMarked.replace(timestamp_token, timestamp)
                else:
                    outLine = timestamp + ' ' + inLineMarked

                if ((datarate_gbph != 0) and \
                        (bytesThisSecond > bytesPerSecondMax)):
                    writeTime = time.clock() - lastSleep
                    sleepTime = 1.0 - writeTime
                    if (sleepTime > 0):
                        time.sleep(sleepTime)
                    lastSleep = time.clock()
                    bytesThisSecond = 0
                lineLength = len(outLine)
                bytesThisSecond = bytesThisSecond + lineLength
                bytesWritten = bytesWritten + lineLength
                if ((filesize_gb != 0) and (bytesWritten >= maxBytes)):
                    break
                else:
                    outFile.write(outLine)
                    lineNum += 1
                    globalNum += 1
                if ((time.time() - last_log_event) > 60) and \
                        (logger is not None):
                    if filesize_gb != 0:
                        logger.info(
                            'Datagen %.2f%s done, by file size' % (
                                (float(bytesWritten) / maxBytes) * 100, '%'))
                    if events != 0:
                        logger.info(
                            'Datagen %.2f%s done, by event count' % (
                                (float(lineNum) / events) * 100, '%'))
                    last_log_event = time.time()
            inFile.close()
            outFile.close()
        i += 1
    return (globalNum - 1)


def generate_syslog(
        input_path, output_path, filesize_gb=0, events=0, num_files=1,
        datarate_gbph=0, runtime_s=0, overwrite=True, logger=None, files_to_keep=1):
    """Wrapper for generate_custom, for syslog
    """
    return generate_custom(
        input_path=input_path,
        output_path=output_path,
        filesize_gb=filesize_gb,
        events=events,
        num_files=num_files,
        datarate_gbph=datarate_gbph,
        runtime_s=runtime_s,
        overwrite=overwrite,
        logger=logger,
        files_to_keep=files_to_keep
    )
