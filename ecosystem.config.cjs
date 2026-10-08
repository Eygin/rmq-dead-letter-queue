module.exports = {
  apps: [
    {
      name: 'dlq-watcher',
      script: 'src/index.js',
      cwd: __dirname,
      autorestart: true,
      watch: false,
      kill_timeout: 5000,
      max_restarts: 10,
      restart_delay: 3000,
    },
  ],
};
