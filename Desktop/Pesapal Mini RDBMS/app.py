from flask import Flask, request, jsonify, render_template, redirect
from minidb import MiniRDBMS

app = Flask(__name__)

# Initialize database
db = MiniRDBMS()
db.create_table("users", ["id", "name", "email"], primary_key="id")

# HOME
@app.route("/")
def home():
    return {"message": "MiniRDBMS Flask App Running"}

# UI to display users and forms
@app.route("/ui")
def ui():
    users = db.get_table("users").select()
    return render_template("index.html", users=users)

# ADD USER (from UI form)
@app.route("/add-user", methods=["POST"])
def add_user_form():
    form = request.form
    try:
        db.get_table("users").insert(   [
            form["id"],
            form["name"],
            form["email"]
        ])
    except Exception as e:
        print("Insert Error:", e)
    return redirect("/ui")

# UPDATE USER (from UI form)
@app.route("/update-user", methods=["POST"])
def update_user_form():
    form = request.form
    try:
        db.get_table("users").update(
            where=("id", form["id"]),
            updates={"name": form["name"], "email": form["email"]}
        )
    except Exception as e:
        print("Update Error:", e)
    return redirect("/ui")

# DELETE USER (from UI form)
@app.route("/delete-user", methods=["POST"])
def delete_user_form():
    form = request.form
    try:
        db.get_table("users").delete(where=("id", form["id"]))
    except Exception as e:
        print("Delete Error:", e)
    return redirect("/ui")

# REST API endpoints (JSON)

# CREATE
@app.route("/users", methods=["POST"])
def create_user():
    data = request.json
    try:
        db.get_table("users").insert([
            str(data["id"]),
            data["name"],
            data["email"]
        ])
        return {"status": "User created"}, 201
    except Exception as e:
        return {"error": str(e)}, 400

# READ
@app.route("/users", methods=["GET"])
def get_users():
    users = db.get_table("users").select()
    return jsonify(users)

# UPDATE
@app.route("/users/<user_id>", methods=["PUT"])
def update_user(user_id):
    data = request.json
    db.get_table("users").update(
        ("id", user_id),
        data
    )
    return {"status": "User updated"}

# DELETE
@app.route("/users/<user_id>", methods=["DELETE"])
def delete_user(user_id):
    db.get_table("users").delete(("id", user_id))
    return {"status": "User deleted"}


if __name__ == "__main__":
    app.run()

