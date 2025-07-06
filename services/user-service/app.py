from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/users')
def get_users():
    # A simple dummy response
    users = [
        {'id': 1, 'name': 'John Doe'},
        {'id': 2, 'name': 'Jane Doe'}
    ]
    return jsonify(users)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)