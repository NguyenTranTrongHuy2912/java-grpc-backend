package iuh.fit.grpc.service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import iuh.fit.grpc.chat.SearchUsersRequest;
import iuh.fit.grpc.chat.SearchUsersResponse;
import iuh.fit.grpc.chat.UserSummary;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

import static iuh.fit.grpc.service.Rpc.fail;

/** Tìm người dùng theo tên (chứa từ khóa) hoặc theo email (tiền tố). */
public class UserSearchService {
    private static final Logger log = LoggerFactory.getLogger(UserSearchService.class);

    private static final int MIN_SEARCH_LENGTH = 2;
    private static final int MAX_SEARCH_LENGTH = 100;
    private static final int DEFAULT_SEARCH_LIMIT = 10;
    private static final int MAX_SEARCH_LIMIT = 20;

    private final MongoCollection<Document> userCollection;

    public UserSearchService(MongoDatabase db) {
        this.userCollection = db.getCollection("users");
    }

    public void searchUsers(SearchUsersRequest request, StreamObserver<SearchUsersResponse> out) {
        String requesterId = request.getRequesterId().trim();
        String query = request.getQuery().trim();
        if (requesterId.isEmpty()) {
            fail(out, Status.INVALID_ARGUMENT.withDescription("Thiếu requester_id"));
            return;
        }
        if (query.length() < MIN_SEARCH_LENGTH || query.length() > MAX_SEARCH_LENGTH) {
            fail(out, Status.INVALID_ARGUMENT.withDescription(
                    "Từ khóa phải từ " + MIN_SEARCH_LENGTH + " đến " + MAX_SEARCH_LENGTH + " ký tự"));
            return;
        }
        int limit = request.getLimit() <= 0 ? DEFAULT_SEARCH_LIMIT
                : Math.min(request.getLimit(), MAX_SEARCH_LIMIT);

        try {
            Bson match;
            if (query.contains("@")) {
                // Email được lưu đã chuẩn hóa (chữ thường) => khớp theo tiền tố
                match = Filters.regex("account", "^" + Pattern.quote(AuthServiceImpl.normalizeEmail(query)));
            } else {
                // Tên: chứa từ khóa, không phân biệt hoa thường
                match = Filters.regex("display_name", Pattern.quote(query), "i");
            }

            SearchUsersResponse.Builder builder = SearchUsersResponse.newBuilder();
            for (Document u : userCollection
                    .find(Filters.and(match, Filters.ne("_id", requesterId)))
                    .projection(Projections.include("display_name", "account"))
                    .sort(Sorts.ascending("display_name"))
                    .limit(limit)) {
                builder.addUsers(UserSummary.newBuilder()
                        .setUserId(u.getString("_id"))
                        .setDisplayName(u.getString("display_name"))
                        .setEmail(u.getString("account")));
            }
            out.onNext(builder.build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Tìm người dùng thất bại", e);
            fail(out, Status.INTERNAL.withDescription("Không tìm được người dùng"));
        }
    }
}
