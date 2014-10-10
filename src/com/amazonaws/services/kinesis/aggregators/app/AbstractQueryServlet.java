package com.amazonaws.services.kinesis.aggregators.app;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public abstract class AbstractQueryServlet extends HttpServlet {
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        doAction(request, response);
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        doAction(request, response);
    }

    protected abstract void doAction(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException;

    protected void doError(HttpServletResponse response, String message) throws ServletException {
        try {
            response.getWriter().print(message);
            response.setStatus(400);
        } catch (IOException e) {
            throw new ServletException(e);
        }
    }
}
