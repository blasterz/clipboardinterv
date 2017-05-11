import React from 'react';
import ReactDOM from 'react-dom';
import {
  BrowserRouter as Router,
  Route,
  Switch,
} from 'react-router-dom';

import Index from './views/Index';
import NotFound from './views/NotFound';

// All of our CSS
require('../public/css/main.scss');

ReactDOM.render(
  <Router>
    <Switch>
      <Route path="/" exact component={Index} />
      <Route component={NotFound} status={404} />
    </Switch>
  </Router>,
  document.getElementById('root'),
);
