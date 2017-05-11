/* eslint-disable global-require */
import 'dotenv/config';
import reactApp from './views/app';

const routes = (app) => {

  /* example api route */
  app.get('/api/records', require('./api/records')); 

  reactApp(app); // set up react routes
};

export default routes;
/* eslint-enable global-require */
