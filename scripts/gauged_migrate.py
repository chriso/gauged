#!/usr/bin/env python

from gauged import Gauged
import argparse
import logging
import sys


def migrate(options):
    gauged = Gauged(options.uri)
    migrations = gauged.driver.prepare_migrations()
    current_meta = gauged.driver.all_metadata()
    current_version = current_meta['current_version']
    goto_version = gauged.VERSION
    if current_version > goto_version:
        logging.error('You have a newer schema than the current version')
        return 1
    for version, upgrade_script in migrations.iteritems():
        logging.debug('-' * 20)
        logging.debug('from/to version : %s/%s', current_version, version)
        # skip until current version
        if version <= current_version:
            logging.debug('skipping because already done')
            continue
        elif version > goto_version:
            logging.debug('skipping because not needed')
            break
        # if migration is found but empty, skip to next
        if not upgrade_script:
            logging.debug('skipping because migration empty')
            current_version = version
            continue
        # if migration is found, needs to be executed
        try:
            if not isinstance(upgrade_script, list):
                upgrade_script = [upgrade_script]
            for stmt in upgrade_script:
                logging.debug('executing %s' % stmt)
                gauged.driver.cursor.execute(stmt)
                gauged.driver.db.commit()
        except:
            gauged.driver.db.rollback()
            logging.error('failed to execute %s', upgrade_script)
            return 1
        logging.info('successfully migrated to %s', version)
        gauged.driver.set_metadata({'current_version': version})
        current_version = version

if __name__ == '__main__':
    descr = 'Migrate a Gauged database to the latest version'
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument(
        '-d', '--debug',
        help='Enable full DEBUG logs',
        action='store_const', dest='loglevel',
        const=logging.DEBUG,
        default=logging.WARNING
    )
    parser.add_argument(
        '-v', '--verbose',
        help='Enable basic logging',
        action='store_const',
        dest='loglevel', const=logging.INFO
    )
    parser.add_argument(
        '-u', '--uri',
        help='Uri to the Gauged Database',
        action='store',
        required=True
    )
    options = parser.parse_args()
    logging.basicConfig(level=options.loglevel)

    rtn = migrate(options)

    sys.exit(rtn)
