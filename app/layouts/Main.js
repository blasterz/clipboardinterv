import React, { Component, PropTypes } from 'react';
import Helmet from 'react-helmet';

class Main extends Component {

  componentWillMount() {
    window.scrollTo(0, 0);
  }

  render() {
    return (
      <div>
        <Helmet titleTemplate="%s | Project Nurse" defaultTitle="Project Nurse" />
        <div id="container" id="main">
          {this.props.children}
        </div>
      </div>);
  }

}

Main.propTypes = {
  children: PropTypes.oneOfType([
    PropTypes.arrayOf(PropTypes.node),
    PropTypes.node,
  ]),
};

Main.defaultProps = {
  children: null,
};

export default Main;
