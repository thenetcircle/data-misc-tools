package com.thenetcircle.service.data.postgresql.udf;

import org.postgresql.pljava.Session;
import org.postgresql.pljava.SessionManager;
import org.postgresql.pljava.TransactionListener;

import java.sql.SQLException;

public class Setup {
    public static final String FLAG_KEY = "init_flag";

    public static final TransactionListener TL = new TransactionListener() {
        @Override
        public void onAbort(Session session) throws SQLException {
            System.out.println("transaction onAbort");
        }

        @Override
        public void onCommit(Session session) throws SQLException {
            System.out.println("transaction onCommit");
        }

        @Override
        public void onPrepare(Session session) throws SQLException {
            System.out.println("transaction onPrepare");
        }
    };

    public static void initiate() throws SQLException {
        Session session = SessionManager.current();

        Object flagObj = session.getAttribute(FLAG_KEY);
        if (flagObj != null) {
            return;
        }

        session.addTransactionListener(TL);
        session.setAttribute(FLAG_KEY, "INIT");
    }
}
