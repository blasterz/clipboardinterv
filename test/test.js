/* eslint-disable func-names */
import 'babel-polyfill';
import 'dotenv/config';
import Nightmare from 'nightmare';
import server from '../server/server';

require('mocha-generators').install();
require('chai').should();

const port = process.env.PORT || 4783;

describe('Page Load Tests:', function () {
  this.timeout(60000);
  let instance;
  let nightmare;

  before((done) => { // before all of the tests
    instance = server.listen(port, done);
  });

  beforeEach(() => { // before each test
    nightmare = Nightmare({ // create a new nightmare instance
      waitTimeout: 5000,
    });
  });

  afterEach(function* () { // after each test
    yield nightmare.end(); // end the nightmare instance
  });

  after((done) => {
    instance.close(done);
  });

  it('Example Test', function* () {
    let text = yield nightmare
      .goto(`http://localhost:${port}/`)
      .wait(1000)
      .evaluate(() => document.title);
    text.should.equal('Clipboard Health');

    text = yield nightmare
      .evaluate(() => document.querySelector('.main > h1').innerText);
    text.should.equal('Test');
  });
});

/* eslint-enable func-names */
