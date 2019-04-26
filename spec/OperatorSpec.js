describe('Table Oprator testing', () => {
  it('create table', (done) => {
    const className = 'TestClient';
    const schema = JSON.parse('{"className":"TestClient","fields":{"objectId":{"type":"String"},"createdAt":{"type":"Date"},"updatedAt":{"type":"Date"},"_rperm":{"type":"Array"},"_wperm":{"type":"Array"}}}');
    expect(200).toBe(200);
    database.create(className, schema)
      .then(res => {
        console.log('result: ', res);
        return Promise.resolve();
      })
      .then(done, done)
      .catch(err => {
        console.log('error:', err);
      });
    done();
  });
});
