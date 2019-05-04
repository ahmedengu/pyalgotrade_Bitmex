import logging

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


def start(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
                     text="commands: \n/list_all")


def commands_callback(strategy_id, strat):
    def main_commands(bot, update):
        cmd = update.message.text

        reply = 'Id: ' + strategy_id
        if cmd == '/list_all_486':
            reply += " is " + (
                'running' if strat.running else 'on pause') + "\ncommands:\n/status_" + strategy_id + "\n/run_" + strategy_id + '\n/exit_market_' + strategy_id + '\n/exit_limit_' + strategy_id + '\n/pause_' + strategy_id + '\n/reverse_' + strategy_id
        elif cmd == '/status_' + strategy_id:
            reply += " is " + ('running ' if strat.running else 'on pause ') + (
                ',is not reversed' if strat.long else ',is reversed') + ' Cash: ' + str(strat.getResult())
        elif cmd == '/run_' + strategy_id:
            reply += " is running"
            logger.warning('running')
            strat.running = True
        elif cmd == '/exit_market_' + strategy_id:
            reply += " is exiting market and paused"
            logger.warning('exiting market and paused')
            strat.running = False
            strat.cancel_and_exit(True)
            logger.warning('paused')
        elif cmd == '/exit_limit_' + strategy_id:
            reply += " is exiting limit and paused"
            logger.warning('exiting limit and paused')
            strat.running = False
            strat.cancel_and_exit(False)
            logger.warning('paused')
        elif cmd == '/pause_' + strategy_id:
            reply += " is paused"
            logger.warning('paused')
            strat.running = False
        elif cmd == '/reverse_' + strategy_id:
            strat.long = not strat.long
            reply +=  ' is not reversed' if strat.long else ' is reversed'
            logger.warning('reversed')

        if reply != 'Id: ' + strategy_id:
            bot.send_message(chat_id=update.message.chat_id, text=reply)

    return main_commands


def help(bot, update):
    update.message.reply_text("Use /start to see the commands list.")


def error(bot, update, error):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, error)


def unknown(bot, update):
    bot.send_message(chat_id=update.message.chat_id, text="Sorry, I didn't understand that command.")


def start_bot(token, strategy_id, strat):
    # Create the Updater and pass it your bot's token.
    updater = Updater(token)

    updater.dispatcher.add_handler(CommandHandler('start', start))
    callback = commands_callback(strategy_id, strat)
    updater.dispatcher.add_handler(CommandHandler('help', help))
    updater.dispatcher.add_handler(MessageHandler(Filters.command, callback))

    updater.dispatcher.add_error_handler(error)

    # Start the Bot
    updater.start_polling()
    return updater
