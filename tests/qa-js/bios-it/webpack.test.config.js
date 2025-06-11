module.exports = {
  mode: 'development',
  module: {
    rules: [
      {
        test: /test\/.*\.js$/,
        exclude: /(node_modules)/,
        use: {
          loader: 'babel-loader',
          options: {
            presets: ['babel-preset-env']
          }
        }
      }
    ]
  },
  resolve: {
    modules: ["node_modules", './'],
    extensions: [".js"]
  }
};
