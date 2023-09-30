CREATE TABLE IF NOT EXISTS Users (
    userId serial4 PRIMARY KEY,
    age INTEGER
);

CREATE TABLE IF NOT EXISTS Items (
    itemId serial4 PRIMARY KEY,
    price NUMERIC(6, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS Purchases (
    purchaseId serial4 PRIMARY KEY,
    userId INTEGER NOT NULL,
    itemId INTEGER NOT NULL,
    "date" TIMESTAMP NOT NULL,
    FOREIGN KEY (userId) REFERENCES public.Users(userId),
    FOREIGN KEY (itemId) REFERENCES public.Items(itemId)
);