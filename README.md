# Giveaway Bot

## Overview
The Giveaway Bot is a Telegram bot designed to facilitate giveaways and manage user interactions. It allows users to register, participate in tasks, and track their balances and referrals.

## Features
- User registration and profile management
- Daily login rewards
- Task management for earning rewards
- Admin panel for user management and statistics
- Integration with PostgreSQL for data storage

## Files
- **bot.py**: Contains the main logic for the bot, including command and conversation handlers, and database interactions.
- **requirements.txt**: Lists the required Python packages for the project.
- **render.yaml**: Configuration file for deploying the bot on the Render platform.
- **README.md**: Documentation for setup and usage.

## Setup Instructions
1. Clone the repository:
   ```
   git clone <repository-url>
   cd giveaway_bot
   ```

2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

3. Configure your PostgreSQL database connection in `bot.py` by updating the `POSTGRES_URL` variable.

4. Set your Telegram bot token in `bot.py` by updating the `BOT_TOKEN` variable.

5. Run the bot:
   ```
   python bot.py
   ```

## Usage
- Start the bot by sending the `/start` command in your Telegram chat.
- Follow the prompts to register and participate in giveaways.
- Admins can access the admin panel using the `/adminpanel` command.

## Contributing
Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.# giveaway_bot
