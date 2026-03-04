from flask import Flask, render_template, request, redirect

app = Flask(__name__)

transactions = []

@app.route('/')
def index():
    total_income = sum(t['amount'] for t in transactions if t['type'] == 'Income')
    total_expense = sum(t['amount'] for t in transactions if t['type'] == 'Expense')
    balance = total_income - total_expense

    return render_template(
        'index.html',
        transactions=transactions,
        total_income=total_income,
        total_expense=total_expense,
        balance=balance
    )

@app.route('/add', methods=['POST'])
def add():
    description = request.form['description']
    amount = float(request.form['amount'])
    t_type = request.form['type']

    transactions.append({
        'description': description,
        'amount': amount,
        'type': t_type
    })

    return redirect('/')

if __name__ == '__main__':
    # IMPORTANT: Needed for Docker
    app.run(host='0.0.0.0', port=5000)
