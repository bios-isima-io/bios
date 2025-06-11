/*
 * Set up database 'admin'
 */
adminDb = connect('mongodb://localhost/admin');

// This use is used by QA test programs to make changes to DB
adminDb.createUser({user: 'mongoadmin', pwd: 'Unbreakable!', roles: ["dbOwner"]});

/*
 * Set up database 'shopping'
 */
shoppingDb = connect('mongodb://localhost/shopping');

// User for login authentication
shoppingDb.createUser({user: "mongodbuser", pwd: "%Test&123!", roles: ["read"]});

// User for X509 client cert authentication
shoppingDb.getSiblingDB("$external").runCommand({
  createUser: "CN=Deli,O=Test",
  roles: [{ role: "read", db: "shopping"}],
});

// These collections have to be created explicitly to enable pre images on changes
shoppingDb.createCollection('cart_items', {changeStreamPreAndPostImages: {enabled: true}});
shoppingDb.createCollection('order_status', {changeStreamPreAndPostImages: {enabled: true}});
shoppingDb.createCollection('stock', {changeStreamPreAndPostImages: {enabled: true}});
