package extra.methods;

import java.util.LinkedList;

public class NewList extends LinkedList<String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String toString() {
		StringBuilder string = new StringBuilder();
		String[] s = null;
		for (int i = 0; i < this.size() - 1; i++) {
			s = this.get(i).split(" ");
			if (s.length == 3) {
				string.append(s[0] + " " + s[1] + " " + s[2]);
			} else {
				string.append(s[0] + " " + s[1]);
			}
			string.append("\n");
		}
		if (!this.isEmpty()) {
			s = this.get(this.size() - 1).split(" ");
			if (s.length == 3) {
				string.append(s[0] + " " + s[1] + " " + s[2]);
			} else {
				string.append(s[0] + " " + s[1]);
			}
		}
		return string.toString();
	}
}
